package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.http.{Client, Paginator}
import fs2.Stream
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.http4s.Uri

import scala.jdk.CollectionConverters._

/** Zero-copy columnar reader that wraps Arrow VectorSchemaRoot as Spark ColumnarBatch. */
class RESTColumnarPartitionReader(partition: RESTInputPartition) extends PartitionReader[ColumnarBatch] {

  private val allocator = new RootAllocator()
  private val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  private val batches: Iterator[ColumnarBatch] = fetchBatches()
  private var currentBatch: ColumnarBatch = _

  override def next(): Boolean = {
    if (batches.hasNext) {
      if (currentBatch != null) currentBatch.close()
      currentBatch = batches.next()
      true
    } else {
      false
    }
  }

  override def get(): ColumnarBatch = currentBatch

  override def close(): Unit = {
    if (currentBatch != null) {
      currentBatch.close()
    }
    allocator.close()
  }

  private def fetchBatches(): Iterator[ColumnarBatch] = {
    val baseUri = Uri.unsafeFromString(partition.baseUrl + partition.endpoint.path)
    val dataPath = partition.tableConfig.flatMap(_.dataPath)

    val program: IO[List[ColumnarBatch]] = Client
      .resource(partition.sourceConfig.http, partition.sourceConfig.auth)
      .use { client =>
        Paginator
          .pages(client, baseUri, partition.pushedParams, partition.sourceConfig.pagination, partition.pushedLimit)
          .flatMap { pageJson =>
            val records = Converter.extractRecords(pageJson, dataPath)
            if (records.isEmpty) Stream.empty
            else {
              val root = Converter.toArrow(records, arrowSchema, allocator)
              val batch = arrowToBatch(root)
              Stream.emit(batch)
            }
          }
          .compile
          .toList
      }

    program.unsafeRunSync().iterator
  }

  /** Wrap Arrow VectorSchemaRoot as ColumnarBatch with zero-copy ArrowColumnVector wrappers. */
  private def arrowToBatch(root: VectorSchemaRoot): ColumnarBatch = {
    val vectors = root.getFieldVectors.asScala.map { v =>
      new ArrowColumnVector(v)
    }.toArray
    val batch = new ColumnarBatch(vectors.asInstanceOf[Array[org.apache.spark.sql.vectorized.ColumnVector]])
    batch.setNumRows(root.getRowCount)
    batch
  }
}

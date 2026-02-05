package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.http.{Client, Paginator}
import io.circe.Json
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.http4s.Uri

import scala.jdk.CollectionConverters._

/** Zero-copy columnar reader for exploded array views.
  * Each API record is exploded by the array field, then batched into Arrow ColumnarBatch.
  */
class ExplodedArrayColumnarPartitionReader(partition: ExplodedArrayInputPartition)
    extends PartitionReader[ColumnarBatch] {

  private val allocator = new RootAllocator()
  private val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  // Eagerly materialized — we must track all roots so close() can release unconsumed ones.
  private val allBatches: List[(ColumnarBatch, VectorSchemaRoot)] = fetchBatches()
  private val batchIterator: Iterator[(ColumnarBatch, VectorSchemaRoot)] = allBatches.iterator
  private var currentIndex: Int = -1

  override def next(): Boolean = {
    if (batchIterator.hasNext) {
      val (_, _) = batchIterator.next()
      currentIndex += 1
      true
    } else {
      false
    }
  }

  override def get(): ColumnarBatch = allBatches(currentIndex)._1

  override def close(): Unit = {
    allBatches.foreach { case (batch, root) =>
      batch.close()
      root.close()
    }
    allocator.close()
  }

  private def fetchBatches(): List[(ColumnarBatch, VectorSchemaRoot)] = {
    val baseUri = Uri.unsafeFromString(partition.baseUrl + partition.endpoint.path)
    val dataPath = partition.tableConfig.flatMap(_.dataPath)
    val batchSize = partition.sourceConfig.schema.arrowBatchSize

    val program: IO[List[(ColumnarBatch, VectorSchemaRoot)]] = Client
      .resource(partition.sourceConfig.http, partition.sourceConfig.auth)
      .use { client =>
        Paginator
          .pages(client, baseUri, Map.empty, partition.sourceConfig.pagination, None)
          .flatMap { pageJson =>
            val records = Converter.extractRecords(pageJson, dataPath)
            val exploded = records.flatMap(explodeRecord)
            if (exploded.isEmpty) fs2.Stream.empty
            else {
              val chunks = exploded.grouped(batchSize).toList
              fs2.Stream.emits(chunks.map { chunk =>
                val root = Converter.toArrow(chunk, arrowSchema, allocator)
                (arrowToBatch(root), root)
              })
            }
          }
          .compile
          .toList
      }

    program.unsafeRunSync()
  }

  /** Explode a single record by its array field.
    * For each element in the array, create a new JSON object with:
    * - All parent scalar fields (excluding the array field)
    * - The array element (as the array field name for primitives, or flattened for objects)
    */
  private def explodeRecord(record: Json): List[Json] = {
    val obj = record.asObject.getOrElse(return Nil)

    // Navigate to the array field using the JSON path
    val arrayJson = partition.arrayJsonPath.foldLeft(record) { (current, key) =>
      current.asObject.flatMap(_.apply(key)).getOrElse(Json.Null)
    }

    val arrayElements = arrayJson.asArray.getOrElse(return Nil)
    if (arrayElements.isEmpty) return Nil

    // Parent fields: all fields except the array field
    val parentFields = obj.toMap - partition.arrayFieldName

    arrayElements.toList.map { element =>
      // Create exploded record: parent fields + array element
      val elementFields = element.asObject match {
        case Some(elemObj) =>
          // For object elements, the fields will be prefixed by arrayFieldName during schema mapping
          Map(partition.arrayFieldName -> element)
        case None =>
          // For primitive elements, just use the array field name
          Map(partition.arrayFieldName -> element)
      }

      Json.fromFields(parentFields ++ elementFields)
    }
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

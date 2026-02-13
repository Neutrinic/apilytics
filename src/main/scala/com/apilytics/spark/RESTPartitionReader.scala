package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.http.{Client, Paginator}
import fs2.Stream
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.http4s.Uri

class RESTPartitionReader(partition: RESTInputPartition) extends PartitionReader[InternalRow] {

  private val allocator = new RootAllocator()
  private val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  // Pages are processed one at a time inside the stream to avoid holding all
  // raw JSON in memory simultaneously. Only compact InternalRows are collected.
  private val rows: Iterator[InternalRow] = fetchAllRows()

  private var currentRow: InternalRow = _

  override def next(): Boolean = {
    if (rows.hasNext) {
      currentRow = rows.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    allocator.close()
  }

  private def fetchAllRows(): Iterator[InternalRow] = {
    val baseUri = Uri.unsafeFromString(partition.baseUrl + partition.endpoint.path)
    val dataPath = partition.tableConfig.flatMap(_.dataPath)

    // Use effective rate limit calculated by RESTScan (distributed across partitions)
    val httpConfig = partition.effectiveRateLimit match {
      case Some(_) => partition.sourceConfig.http.copy(rateLimit = partition.effectiveRateLimit)
      case None    => partition.sourceConfig.http
    }

    val program: IO[List[InternalRow]] = Client
      .resource(httpConfig, partition.sourceConfig.auth)
      .use { client =>
        Paginator
          .pages(client, baseUri, partition.pushedParams, partition.sourceConfig.pagination, partition.pushedLimit)
          .flatMap { pageJson =>
            val records = Converter.extractRecords(pageJson, dataPath)
            if (records.isEmpty) Stream.empty
            else {
              val root = Converter.toArrow(records, arrowSchema, allocator)
              val rows = try {
                ArrowUtils.arrowToInternalRows(root)
              } finally {
                root.close()
              }
              Stream.emits(rows)
            }
          }
          .compile
          .toList
      }

    program.unsafeRunSync().iterator
  }
}

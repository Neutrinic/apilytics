package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.http.{Client, Paginator}
import io.circe.Json
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String
import org.http4s.Uri

import scala.jdk.CollectionConverters._

class RESTPartitionReader(partition: RESTInputPartition) extends PartitionReader[InternalRow] {

  private val allocator = new RootAllocator()

  // Collect all pages into rows eagerly.
  // REST APIs are not high-throughput; materializing is fine.
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

    val program: IO[List[InternalRow]] = Client
      .resource(partition.sourceConfig.http, partition.sourceConfig.auth)
      .use { client =>
        Paginator
          .pages(client, baseUri, partition.pushedParams, partition.sourceConfig.pagination, partition.pushedLimit)
          .compile
          .toList
          .map { pages =>
            pages.flatMap { pageJson =>
              val records = Converter.extractRecords(pageJson, dataPath)
              if (records.isEmpty) Nil
              else {
                val root = Converter.toArrow(records, partition.arrowSchema, allocator)
                try {
                  arrowToInternalRows(root)
                } finally {
                  root.close()
                }
              }
            }
          }
      }

    program.unsafeRunSync().iterator
  }

  private def arrowToInternalRows(root: VectorSchemaRoot): List[InternalRow] = {
    val fieldCount = root.getFieldVectors.size()
    val rowCount = root.getRowCount

    (0 until rowCount).map { rowIdx =>
      val values = new Array[Any](fieldCount)
      root.getFieldVectors.asScala.zipWithIndex.foreach { case (vector, colIdx) =>
        values(colIdx) = if (vector.isNull(rowIdx)) {
          null
        } else {
          vector match {
            case v: org.apache.arrow.vector.VarCharVector =>
              UTF8String.fromBytes(v.get(rowIdx))
            case v: org.apache.arrow.vector.IntVector =>
              v.get(rowIdx)
            case v: org.apache.arrow.vector.BigIntVector =>
              v.get(rowIdx)
            case v: org.apache.arrow.vector.Float8Vector =>
              v.get(rowIdx)
            case v: org.apache.arrow.vector.BitVector =>
              v.get(rowIdx) == 1
            case v: org.apache.arrow.vector.DateDayVector =>
              v.get(rowIdx) // days since epoch, Spark DateType uses int
            case v: org.apache.arrow.vector.TimeStampMicroTZVector =>
              v.get(rowIdx) // micros since epoch, Spark TimestampType uses long
            case _ => null
          }
        }
      }
      new GenericInternalRow(values)
    }.toList
  }
}

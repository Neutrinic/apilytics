package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.http.{Client, Paginator}
import io.circe.Json
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String
import org.http4s.Uri

import scala.jdk.CollectionConverters._

/** Row-based partition reader for parent-child endpoint joins.
  *
  * NOTE: This reader eagerly materializes all rows into memory via `.compile.toList`.
  * It exists as a debug/test fallback only — production execution uses the columnar
  * reader (ParentChildColumnarPartitionReader) which streams lazily via LazyColumnarReader.
  * Since supportColumnarReads always returns true, Spark will never instantiate this
  * reader in normal execution.
  */
class ParentChildPartitionReader(partition: ParentChildInputPartition) extends PartitionReader[InternalRow] {

  private val allocator = new RootAllocator()
  private val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  private val rows: Iterator[InternalRow] = fetchParentChildRows()
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

  private def fetchParentChildRows(): Iterator[InternalRow] = {
    val parentBaseUri = Uri.unsafeFromString(partition.baseUrl + partition.parentEndpoint.path)
    val batchSize = partition.sourceConfig.schema.arrowBatchSize

    val program: IO[List[InternalRow]] = Client
      .resource(partition.sourceConfig.http, partition.sourceConfig.auth)
      .use { client =>
        // Fetch parent records
        val parentPages = Paginator.pages(
          client,
          parentBaseUri,
          Map.empty,
          partition.sourceConfig.pagination,
          None
        )

        parentPages
          .flatMap { pageJson =>
            val records = Converter.extractRecords(pageJson, None)
            fs2.Stream.emits(records)
          }
          .flatMap { parentRecord =>
            // Extract parent key value
            val parentKeyValue = parentRecord.asObject
              .flatMap(_.apply(partition.parentKey))
              .flatMap(v => v.asString.orElse(v.asNumber.map(_.toString)))

            parentKeyValue match {
              case None => fs2.Stream.empty
              case Some(keyValue) =>
                val childPath = partition.childEndpointTemplate.replace(
                  s"{${partition.pathParamName}}",
                  keyValue
                )
                val childUri = Uri.unsafeFromString(partition.baseUrl + childPath)
                val dataPath = partition.tableConfig.dataPath

                Paginator.pages(
                  client,
                  childUri,
                  partition.pushedParams,
                  partition.sourceConfig.pagination,
                  partition.pushedLimit
                ).flatMap { pageJson =>
                  val childRecords = Converter.extractRecords(pageJson, dataPath)
                  if (childRecords.isEmpty) fs2.Stream.empty
                  else {
                    val enrichedRecords = childRecords.map { childRecord =>
                      childRecord.asObject match {
                        case Some(obj) =>
                          Json.fromFields(
                            (partition.parentKeyColumn -> Json.fromString(keyValue)) +: obj.toList
                          )
                        case None =>
                          Json.obj(
                            partition.parentKeyColumn -> Json.fromString(keyValue),
                            "value" -> childRecord
                          )
                      }
                    }

                    val root = Converter.toArrow(enrichedRecords, arrowSchema, allocator)
                    val rows = try {
                      arrowToInternalRows(root)
                    } finally {
                      root.close()
                    }
                    fs2.Stream.emits(rows)
                  }
                }
            }
          }
          .compile
          .toList
      }

    program.unsafeRunSync().iterator
  }

  private def arrowToInternalRows(root: org.apache.arrow.vector.VectorSchemaRoot): List[InternalRow] = {
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
              v.get(rowIdx)
            case v: org.apache.arrow.vector.TimeStampMicroTZVector =>
              v.get(rowIdx)
            case _ => null
          }
        }
      }
      new GenericInternalRow(values)
    }.toList
  }
}

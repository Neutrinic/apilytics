package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.http.{Client, Paginator}
import io.circe.Json
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.http4s.Uri

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
            // Extract parent key JSON value (preserve original type for output column)
            val parentKeyJson = parentRecord.asObject.flatMap(_.apply(partition.parentKey))

            // Convert to string for path substitution
            val parentKeyString = parentKeyJson.flatMap { json =>
              json.asString
                .orElse(json.asNumber.map(_.toString))
                .orElse(json.asBoolean.map(_.toString))
            }

            parentKeyString match {
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
                    // Add parent key column preserving original JSON type
                    val enrichedRecords = childRecords.map { childRecord =>
                      childRecord.asObject match {
                        case Some(obj) =>
                          Json.fromFields(
                            (partition.parentKeyColumn -> parentKeyJson.get) +: obj.toList
                          )
                        case None =>
                          Json.obj(
                            partition.parentKeyColumn -> parentKeyJson.get,
                            "value" -> childRecord
                          )
                      }
                    }

                    val root = Converter.toArrow(enrichedRecords, arrowSchema, allocator)
                    val rows = try {
                      ArrowUtils.arrowToInternalRows(root)
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
}

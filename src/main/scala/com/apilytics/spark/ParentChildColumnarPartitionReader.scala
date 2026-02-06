package com.apilytics.spark

import cats.effect.{IO, Resource}
import com.apilytics.core.arrow.Converter
import com.apilytics.core.http.{Client, Paginator}
import io.circe.Json
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.http4s.Uri

/** Columnar reader for parent-child endpoint joins (nested API calls).
  *
  * Execution strategy (nested loop):
  * 1. Fetch all pages from the parent endpoint
  * 2. For each parent record, extract the parent key value
  * 3. Substitute the key into the child endpoint template
  * 4. Fetch all pages from the child endpoint
  * 5. Add the parent key column to each child record
  * 6. Emit as Arrow batches
  */
class ParentChildColumnarPartitionReader(partition: ParentChildInputPartition)
    extends LazyColumnarReader {

  override protected val allocator: RootAllocator = new RootAllocator()
  override protected val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  override protected def clientResource: Resource[IO, Client.RestClient] =
    Client.resource(partition.sourceConfig.http, partition.sourceConfig.auth)

  override protected def buildStream(
      client: Client.RestClient
  ): fs2.Stream[IO, (ColumnarBatch, VectorSchemaRoot)] = {
    val parentBaseUri = Uri.unsafeFromString(partition.baseUrl + partition.parentEndpoint.path)
    val batchSize = partition.sourceConfig.schema.arrowBatchSize

    // Phase 1: Fetch all parent records
    val parentPages = Paginator.pages(
      client,
      parentBaseUri,
      Map.empty, // TODO: support parent filter pushdown in future
      partition.sourceConfig.pagination,
      None
    )

    // Extract records from parent pages
    val parentRecords: fs2.Stream[IO, Json] = parentPages.flatMap { pageJson =>
      // Parent tables typically don't have a custom data-path, but could in future
      val records = Converter.extractRecords(pageJson, None)
      fs2.Stream.emits(records)
    }

    // Phase 2: For each parent record, fetch child endpoint
    parentRecords.flatMap { parentRecord =>
      // Extract parent key JSON value (preserve original type for output column)
      val parentKeyJson = parentRecord.asObject.flatMap(_.apply(partition.parentKey))

      // Convert to string for path substitution (works for strings, numbers, booleans)
      val parentKeyString = parentKeyJson.flatMap { json =>
        json.asString
          .orElse(json.asNumber.map(_.toString))
          .orElse(json.asBoolean.map(_.toString))
      }

      parentKeyString match {
        case None =>
          // Skip records without the parent key or with null/object/array keys
          fs2.Stream.empty

        case Some(keyValue) =>
          // Substitute key into child endpoint template
          val childPath = partition.childEndpointTemplate.replace(
            s"{${partition.pathParamName}}",
            keyValue
          )
          val childUri = Uri.unsafeFromString(partition.baseUrl + childPath)
          val dataPath = partition.tableConfig.dataPath

          // Fetch child pages for this parent
          val childPages = Paginator.pages(
            client,
            childUri,
            partition.pushedParams,
            partition.sourceConfig.pagination,
            partition.pushedLimit
          )

          // Extract and enrich child records with parent key (preserving original JSON type)
          childPages.flatMap { pageJson =>
            val childRecords = Converter.extractRecords(pageJson, dataPath)
            if (childRecords.isEmpty) fs2.Stream.empty
            else {
              // Add parent key column to each child record, preserving original type
              val enrichedRecords = childRecords.map { childRecord =>
                childRecord.asObject match {
                  case Some(obj) =>
                    Json.fromFields(
                      (partition.parentKeyColumn -> parentKeyJson.get) +: obj.toList
                    )
                  case None =>
                    // Non-object child record - wrap it
                    Json.obj(
                      partition.parentKeyColumn -> parentKeyJson.get,
                      "value" -> childRecord
                    )
                }
              }

              // Convert to Arrow batches
              val chunks = enrichedRecords.grouped(batchSize).toList
              fs2.Stream.emits(chunks.map { chunk =>
                val root = Converter.toArrow(chunk, arrowSchema, allocator)
                (arrowToBatch(root), root)
              })
            }
          }
      }
    }
  }
}

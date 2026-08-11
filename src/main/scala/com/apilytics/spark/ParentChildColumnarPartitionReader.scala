package com.apilytics.spark

import cats.effect.{IO, Resource}
import com.apilytics.core.arrow.Converter
import com.apilytics.core.config.JoinStrategy
import com.apilytics.core.http.{Client, Paginator, ResponseCache}
import io.circe.Json
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.http4s.Uri

/** Columnar reader for parent-child endpoint joins (nested API calls).
  *
  * Supports two execution strategies:
  *
  * 1. Nested Loop (default):
  *    - Fetch all pages from the parent endpoint
  *    - For each parent record, substitute the parent key into the child endpoint path
  *    - Fetch all pages from the child endpoint
  *    - O(n) API calls where n = number of parent rows
  *
  * 2. Batch Join:
  *    - Fetch all pages from the parent endpoint
  *    - Collect parent keys into batches
  *    - For each batch, call child endpoint with batch query param (e.g., ?ids=1,2,3)
  *    - O(n/batch_size) API calls
  */
class ParentChildColumnarPartitionReader(partition: ParentChildInputPartition)
    extends LazyColumnarReader {

  // Use lazy vals to avoid initialization order issues with LazyColumnarReader's constructor
  // which starts a fiber that may access these before they're initialized
  override protected lazy val allocator: RootAllocator = new RootAllocator()
  override protected lazy val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  override protected def prefetchSize: Int = partition.sourceConfig.schema.prefetchBatches

  // Use lazy vals to avoid initialization order issues with LazyColumnarReader's constructor
  // which starts a fiber that may access these before they're initialized
  private lazy val responseCache = ResponseCache.fromConfig(partition.sourceConfig.http.responseCache)

  // Use effective rate limit calculated by ParentChildScan (distributed across partitions)
  private lazy val httpConfig = partition.effectiveRateLimit match {
    case Some(_) => partition.sourceConfig.http.copy(rateLimit = partition.effectiveRateLimit)
    case None    => partition.sourceConfig.http
  }

  override protected def clientResource: Resource[IO, Client.RestClient] =
    Client.resource(httpConfig, partition.sourceConfig.auth, responseCache)

  override protected def buildStream(
      client: Client.RestClient
  ): fs2.Stream[IO, (ColumnarBatch, VectorSchemaRoot)] = {
    val parentBaseUri = Uri.unsafeFromString(partition.baseUrl + partition.parentEndpoint.path)
    val batchSize = partition.sourceConfig.schema.arrowBatchSize

    // Fetch all parent records first
    val parentPages = Paginator.pages(
      client,
      parentBaseUri,
      Map.empty,
      partition.sourceConfig.pagination,
      None
    )
    val parentRecords: fs2.Stream[IO, Json] = parentPages.flatMap { pageJson =>
      val records = Converter.extractRecords(pageJson, None)
      fs2.Stream.emits(records)
    }

    // Determine join strategy (default to NestedLoop for backward compatibility)
    val joinStrategy = partition.tableConfig.joinStrategy.getOrElse(JoinStrategy.NestedLoop)

    joinStrategy match {
      case JoinStrategy.Batch =>
        executeBatchJoin(parentRecords, client, batchSize)
      case JoinStrategy.NestedLoop =>
        executeNestedLoopJoin(parentRecords, client, batchSize)
    }
  }

  /** Execute nested loop join - one API call per parent row. */
  private def executeNestedLoopJoin(
      parentRecords: fs2.Stream[IO, Json],
      client: Client.RestClient,
      batchSize: Int
  ): fs2.Stream[IO, (ColumnarBatch, VectorSchemaRoot)] = {
    parentRecords.flatMap { parentRecord =>
      val parentKeyJson = parentRecord.asObject.flatMap(_.apply(partition.parentKey))
      val parentKeyString = parentKeyJson.flatMap(ParentChildUtils.jsonToString)

      parentKeyString match {
        case None =>
          fs2.Stream.empty

        case Some(keyValue) =>
          val childPath = partition.childEndpointTemplate.replace(
            s"{${partition.pathParamName}}",
            keyValue
          )
          val childUri = Uri.unsafeFromString(partition.baseUrl + childPath)
          val dataPath = partition.tableConfig.dataPath

          val childPages = Paginator.pages(
            client,
            childUri,
            partition.pushedParams,
            partition.sourceConfig.pagination,
            partition.pushedLimit
          )
          childPages.flatMap { pageJson =>
            val childRecords = Converter.extractRecords(pageJson, dataPath)
            if (childRecords.isEmpty) fs2.Stream.empty
            else {
              val enrichedRecords = childRecords.map { childRecord =>
                ParentChildUtils.enrichChildRecord(childRecord, parentKeyJson.get, partition.parentKeyColumn)
              }
              // Convert in `map`, not inside `emits`, so batches are allocated as the
              // bounded queue pulls them rather than all at once per page.
              fs2.Stream.emits(enrichedRecords.grouped(batchSize).toList).unchunk.map { chunk =>
                val root = Converter.toArrow(chunk, arrowSchema, allocator)
                (arrowToBatch(root), root)
              }
            }
          }
      }
    }
  }

  /** Execute batch join - collect parent keys and fetch children in batches. */
  private def executeBatchJoin(
      parentRecords: fs2.Stream[IO, Json],
      client: Client.RestClient,
      arrowBatchSize: Int
  ): fs2.Stream[IO, (ColumnarBatch, VectorSchemaRoot)] = {
    val batchParam = partition.tableConfig.batchParam.getOrElse(
      throw new IllegalArgumentException(
        "Batch join strategy requires 'batch-param' in table config (e.g., 'ids')"
      )
    )
    val maxBatchSize = partition.tableConfig.batchSize
    val separator = partition.tableConfig.batchSeparator

    // For batch joins, the endpoint should NOT have path parameter substitution
    // It should be a base endpoint like "/orders" not "/orders/{order_id}/items"
    val childBaseUri = Uri.unsafeFromString(partition.baseUrl + partition.tableConfig.endpoint)
    val dataPath = partition.tableConfig.dataPath

    // Collect all parent keys with their original JSON values
    parentRecords
      .map { parentRecord =>
        val parentKeyJson = parentRecord.asObject.flatMap(_.apply(partition.parentKey))
        val parentKeyString = parentKeyJson.flatMap(ParentChildUtils.jsonToString)
        (parentKeyString, parentKeyJson)
      }
      .collect { case (Some(keyStr), Some(keyJson)) => (keyStr, keyJson) }
      .chunkN(maxBatchSize)
      .flatMap { batchChunk =>
        val batch = batchChunk.toList
        // toMap silently drops duplicate parent keys, but this is safe because
        // we only use the map to look up JSON values for enrichment, and duplicate
        // keys map to the same value anyway.
        val parentKeysMap = batch.toMap
        val keyValues = batch.map(_._1).mkString(separator)

        val batchParams = partition.pushedParams + (batchParam -> keyValues)

        val childPages = Paginator.pages(
          client,
          childBaseUri,
          batchParams,
          partition.sourceConfig.pagination,
          partition.pushedLimit
        )
        childPages.flatMap { pageJson =>
          val childRecords = Converter.extractRecords(pageJson, dataPath)
          if (childRecords.isEmpty) fs2.Stream.empty
          else {
            // Find the parent key field in child records to map back
            // The API should return records with a field matching the parent key
            val enrichedRecords = childRecords.flatMap { childRecord =>
              ParentChildUtils.findParentKeyForChild(
                childRecord,
                parentKeysMap,
                partition.parentKey,
                partition.tableConfig.childKeyField
              ) match {
                case Some(parentKeyJson) =>
                  List(ParentChildUtils.enrichChildRecord(childRecord, parentKeyJson, partition.parentKeyColumn))
                case None =>
                  // Child record doesn't match any parent in this batch - skip
                  // Or we could include with null parent key based on config
                  List.empty
              }
            }

            if (enrichedRecords.isEmpty) fs2.Stream.empty
            else {
              // Convert in `map`, not inside `emits`, so batches are allocated as the
              // bounded queue pulls them rather than all at once per batch-join round.
              fs2.Stream.emits(enrichedRecords.grouped(arrowBatchSize).toList).unchunk.map { chunk =>
                val root = Converter.toArrow(chunk, arrowSchema, allocator)
                (arrowToBatch(root), root)
              }
            }
          }
        }
      }
  }
}

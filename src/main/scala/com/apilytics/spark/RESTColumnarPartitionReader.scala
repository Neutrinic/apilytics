package com.apilytics.spark

import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.checkpoint.{CheckpointState, CheckpointStore}
import com.apilytics.core.config.{CheckpointMode, ResponseFormat, SchemaMode}
import com.apilytics.core.http.{Client, Paginator, ResponseCache}
import fs2.Stream
import io.circe.Json
import io.circe.pointer.Pointer
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.http4s.Uri

/** Zero-copy columnar reader that lazily streams Arrow batches via a bounded queue.
  * Memory scales with batch size, not total partition size.
  *
  * For variant mode, bypasses Arrow entirely and uses native Spark VariantType
  * for efficient semi-structured data queries with path navigation syntax.
  *
  * When checkpoint is enabled, loads the last saved state on init and persists
  * the final state on close() for incremental reads.
  */
class RESTColumnarPartitionReader(partition: RESTInputPartition) extends LazyColumnarReader with Logging {

  override protected val allocator: RootAllocator = new RootAllocator()
  override protected val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  // Use lazy vals to avoid initialization order issues with LazyColumnarReader's constructor
  // which starts a fiber that may access these before they're initialized
  private lazy val responseCache = ResponseCache.fromConfig(partition.sourceConfig.http.responseCache)

  // Use effective rate limit calculated by RESTScan (distributed across partitions)
  private lazy val httpConfig = partition.effectiveRateLimit match {
    case Some(_) => partition.sourceConfig.http.copy(rateLimit = partition.effectiveRateLimit)
    case None    => partition.sourceConfig.http
  }

  // Checkpoint support
  private lazy val checkpointStore: CheckpointStore =
    CheckpointStore.fromConfig(partition.checkpointConfig, new Configuration())

  private lazy val startState: Option[CheckpointState] =
    checkpointStore.read(partition.tableName).unsafeRunSync()

  @volatile private var lastState: Option[CheckpointState] = None

  override protected def clientResource: Resource[IO, Client.RestClient] =
    Client.resource(httpConfig, partition.sourceConfig.auth, responseCache)

  override protected def buildStream(
      client: Client.RestClient
  ): Stream[IO, (org.apache.spark.sql.vectorized.ColumnarBatch, VectorSchemaRoot)] = {
    val baseUri = Uri.unsafeFromString(partition.baseUrl + partition.endpoint.path)
    val batchSize = partition.sourceConfig.schema.arrowBatchSize

    // For streaming formats (NDJSON, SSE), each record from the stream is already
    // an individual JSON object - data-path extraction doesn't apply. data-path is
    // only used for standard JSON responses where records are nested in a wrapper
    // (e.g., {"results": [...], "pagination": {...}}).
    val isStreamingFormat = partition.responseFormat != ResponseFormat.Json
    val dataPath = if (isStreamingFormat) None else partition.tableConfig.flatMap(_.dataPath)

    // For timestamp checkpoint mode, inject the saved timestamp as a query parameter
    val checkpointParams = injectTimestampParam(partition.pushedParams)

    Paginator
      .pages(client, baseUri, checkpointParams, partition.sourceConfig.pagination, partition.pushedLimit, partition.responseFormat, startState)
      .flatMap { case (pageJson, pageState) =>
        // Track checkpoint state - update for cursor/offset modes
        pageState.foreach(s => lastState = Some(s))

        // For timestamp mode, extract timestamp from records
        if (partition.checkpointConfig.exists(cc => cc.enabled && cc.mode == CheckpointMode.Timestamp)) {
          extractTimestampState(pageJson, dataPath).foreach(s => lastState = Some(s))
        }

        val records = Converter.extractRecords(pageJson, dataPath)
        if (records.isEmpty) Stream.empty
        else {
          val chunks = records.grouped(batchSize).toList
          partition.schemaMode match {
            case SchemaMode.Variant =>
              // Native VARIANT path: bypass Arrow, convert JSON directly to VariantType
              Stream.emits(chunks.map { chunk =>
                // Return (batch, null) - no VectorSchemaRoot to close for variant path
                (VariantBatch.fromRecords(chunk), null)
              })
            case _ =>
              // Standard path: use Arrow for typed schema
              Stream.emits(chunks.map { chunk =>
                val root = Converter.toArrow(chunk, arrowSchema, allocator)
                (arrowToBatch(root), root)
              })
          }
        }
      }
  }

  override def close(): Unit = {
    // Persist checkpoint state before closing
    lastState.foreach { state =>
      if (partition.checkpointConfig.exists(_.enabled)) {
        logInfo(s"Saving checkpoint for table '${partition.tableName}': ${state.toJson.noSpaces}")
        checkpointStore.write(partition.tableName, state).unsafeRunSync()
      }
    }
    super.close()
  }

  /** Inject saved timestamp as a query parameter for timestamp checkpoint mode. */
  private def injectTimestampParam(params: Map[String, String]): Map[String, String] = {
    (partition.checkpointConfig, startState) match {
      case (Some(cc), Some(CheckpointState.TimestampValue(ts)))
          if cc.enabled && cc.mode == CheckpointMode.Timestamp && cc.timestampParam.isDefined =>
        logInfo(s"Resuming from checkpoint timestamp: $ts (param: ${cc.timestampParam.get})")
        params + (cc.timestampParam.get -> ts)
      case _ => params
    }
  }

  /** Extract the latest timestamp from records for timestamp checkpoint mode. */
  private def extractTimestampState(pageJson: Json, dataPath: Option[String]): Option[CheckpointState] = {
    partition.checkpointConfig.flatMap { cc =>
      cc.timestampPath.flatMap { tsPath =>
        Pointer.parse(tsPath).toOption.flatMap { pointer =>
          val records = Converter.extractRecords(pageJson, dataPath)
          records.lastOption.flatMap { lastRecord =>
            pointer.get(lastRecord).toOption.flatMap(_.asString).map(CheckpointState.TimestampValue)
          }
        }
      }
    }
  }
}

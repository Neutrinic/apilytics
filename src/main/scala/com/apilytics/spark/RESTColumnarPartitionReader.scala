package com.apilytics.spark

import cats.effect.{IO, Ref, Resource}
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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.http4s.Uri

/** Zero-copy columnar reader that lazily streams Arrow batches via a bounded queue.
  * Memory scales with batch size, not total partition size.
  *
  * For variant mode, bypasses Arrow entirely and uses native Spark VariantType
  * for efficient semi-structured data queries with path navigation syntax.
  *
  * When checkpoint is enabled, loads the last saved state before pagination starts
  * and persists the final state via stream onFinalize — all within the IO context,
  * avoiding cross-thread mutation.
  */
class RESTColumnarPartitionReader(partition: RESTInputPartition) extends LazyColumnarReader with Logging {

  override protected val allocator: RootAllocator = new RootAllocator()
  override protected val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  override protected def prefetchSize: Int = partition.sourceConfig.schema.prefetchBatches

  // Use lazy vals to avoid initialization order issues with LazyColumnarReader's constructor
  // which starts a fiber that may access these before they're initialized
  private lazy val responseCache = ResponseCache.fromConfig(partition.sourceConfig.http.responseCache)

  // Use effective rate limit calculated by RESTScan (distributed across partitions)
  private lazy val httpConfig = partition.effectiveRateLimit match {
    case Some(_) => partition.sourceConfig.http.copy(rateLimit = partition.effectiveRateLimit)
    case None    => partition.sourceConfig.http
  }

  // Use Spark's Hadoop config for S3/HDFS credential resolution via core-site.xml.
  //
  // Executors normally have no active session, so the fallback is the common path,
  // not an edge case. Ask via getActiveSession/getDefaultSession rather than
  // SparkSession.active: `active` throws when there is no session, and the type it
  // throws changed in Spark 4.1 (IllegalStateException -> SparkException), which
  // silently defeated the previous try/catch.
  private lazy val checkpointStore: CheckpointStore = {
    val hadoopConf = SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .map(_.sparkContext.hadoopConfiguration)
      .getOrElse(new org.apache.hadoop.conf.Configuration())
    CheckpointStore.fromConfig(partition.checkpointConfig, hadoopConf)
  }

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

    val isTimestampMode = partition.checkpointConfig.exists(cc => cc.enabled && cc.mode == CheckpointMode.Timestamp)
    val timestampPointer = if (isTimestampMode) {
      partition.checkpointConfig.flatMap(_.timestampPath).flatMap(p => Pointer.parse(p).toOption)
    } else None

    // All checkpoint IO happens within the stream's effect context:
    // 1. Read start state from store
    // 2. Create Ref for atomic state tracking
    // 3. Paginate with state tracking
    // 4. Persist final state via onFinalize
    Stream.eval(checkpointStore.read(partition.tableName)).flatMap { startState =>
      // For timestamp checkpoint mode, inject the saved timestamp as a query parameter
      val checkpointParams = injectTimestampParam(partition.pushedParams, startState)

      Stream.eval(Ref.of[IO, Option[CheckpointState]](None)).flatMap { stateRef =>
        Paginator
          .pagesWithState(client, baseUri, checkpointParams, partition.sourceConfig.pagination,
            partition.pushedLimit, partition.responseFormat, startState)
          .flatMap { case (pageJson, pageState) =>
            val records = Converter.extractRecords(pageJson, dataPath)

            // Determine checkpoint state for this page:
            // - For cursor/offset modes, use the state from Paginator
            // - For timestamp mode, extract max timestamp from records
            val effectiveState = if (isTimestampMode) {
              extractMaxTimestamp(records, timestampPointer).orElse(pageState)
            } else {
              pageState
            }

            // Update checkpoint ref within the IO context
            val updateState = effectiveState match {
              case Some(s) => stateRef.set(Some(s))
              case None    => IO.unit
            }

            if (records.isEmpty) Stream.eval(updateState).drain
            else {
              Stream.eval(updateState) >>
              // Emit the chunks first and convert in `map`, so each Arrow root is
              // built only when the bounded queue pulls it. Converting inside
              // `Stream.emits` would allocate every batch in the page up front and
              // make peak memory track page size instead of arrow-batch-size.
              Stream.emits(records.grouped(batchSize).toList).unchunk.map { chunk =>
                partition.schemaMode match {
                  case SchemaMode.Variant =>
                    (VariantBatch.fromRecords(chunk), null: VectorSchemaRoot)
                  case _ =>
                    val root = Converter.toArrow(chunk, arrowSchema, allocator)
                    (arrowToBatch(root), root)
                }
              }
            }
          }
          .onFinalize {
            stateRef.get.flatMap {
              case Some(state) if partition.checkpointConfig.exists(_.enabled) =>
                IO(logInfo(s"Saving checkpoint for table '${partition.tableName}': ${state.toJson.noSpaces}")) >>
                  checkpointStore.write(partition.tableName, state)
              case _ => IO.unit
            }
          }
      }
    }
  }

  /** Inject saved timestamp as a query parameter for timestamp checkpoint mode. */
  private def injectTimestampParam(params: Map[String, String], startState: Option[CheckpointState]): Map[String, String] = {
    (partition.checkpointConfig, startState) match {
      case (Some(cc), Some(CheckpointState.TimestampValue(ts)))
          if cc.enabled && cc.mode == CheckpointMode.Timestamp && cc.timestampParam.isDefined =>
        logInfo(s"Resuming from checkpoint timestamp: $ts (param: ${cc.timestampParam.get})")
        params + (cc.timestampParam.get -> ts)
      case _ => params
    }
  }

  /** Extract the maximum timestamp from already-extracted records.
    *
    * Uses lexicographic max (`String#max`), which gives correct temporal ordering
    * only for ISO-8601 strings with fixed-width components and consistent timezone
    * (e.g., "2024-01-15T10:30:00Z"). Non-ISO formats such as Unix epoch strings,
    * non-zero-padded dates, or varying timezone offsets will compare incorrectly.
    */
  private def extractMaxTimestamp(records: List[Json], pointer: Option[Pointer]): Option[CheckpointState] = {
    pointer.flatMap { ptr =>
      val timestamps = records.flatMap { record =>
        ptr.get(record).toOption.flatMap(_.asString)
      }
      if (timestamps.isEmpty) None
      else Some(CheckpointState.TimestampValue(timestamps.max))
    }
  }
}

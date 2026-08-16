package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.config.JoinStrategy
import com.apilytics.core.rest.{RestHandle, RestSource}
import com.apilytics.core.source.{ReadRequest, RecordSession}
import io.circe.Json
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

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

  /** Handle for a path on this source. `tableConfig = None` means no data-path
    * extraction, which is what the parent endpoint wants. */
  private def handleFor(path: String, withTableConfig: Boolean = false) =
    RestHandle(path, partition.baseUrl, if (withTableConfig) Some(partition.tableConfig) else None)

  private def fetchParentChildRows(): Iterator[InternalRow] = {
    val batchSize = partition.sourceConfig.schema.arrowBatchSize
    val joinStrategy = partition.tableConfig.joinStrategy.getOrElse(JoinStrategy.NestedLoop)

    // This partition's share of the rate limit, distributed by ParentChildScan (#205).
    val effectiveConfig = partition.effectiveRateLimit match {
      case Some(_) =>
        partition.sourceConfig.copy(
          http = partition.sourceConfig.http.copy(rateLimit = partition.effectiveRateLimit)
        )
      case None => partition.sourceConfig
    }

    val program: IO[List[InternalRow]] = new RestSource(effectiveConfig).session
      .use { session =>
        // Fetch parent records. No table config on the handle means no data-path
        // extraction, which is what the parent endpoint wants.
        val parentRecords: fs2.Stream[IO, Json] =
          session
            .pages(ReadRequest(handleFor(partition.parentEndpoint.path)))
            .flatMap(page => fs2.Stream.emits(page.records))

        val rowsStream = joinStrategy match {
          case JoinStrategy.Batch =>
            executeBatchJoin(parentRecords, session, batchSize)
          case JoinStrategy.NestedLoop =>
            executeNestedLoopJoin(parentRecords, session, batchSize)
        }
        rowsStream.compile.toList
      }

    program.unsafeRunSync().iterator
  }

  private def executeNestedLoopJoin(
      parentRecords: fs2.Stream[IO, Json],
      session: RecordSession,
      batchSize: Int
  ): fs2.Stream[IO, InternalRow] = {
    parentRecords.flatMap { parentRecord =>
      val parentKeyJson = parentRecord.asObject.flatMap(_.apply(partition.parentKey))
      val parentKeyString = parentKeyJson.flatMap(ParentChildUtils.jsonToString)

      parentKeyString match {
        case None => fs2.Stream.empty
        case Some(keyValue) =>
          val childPath = partition.childEndpointTemplate.replace(
            s"{${partition.pathParamName}}",
            keyValue
          )


          session.pages(ReadRequest(
            handleFor(childPath, withTableConfig = true),
            partition.pushedParams,
            partition.pushedLimit
          )).flatMap { page =>
            val childRecords = page.records
            if (childRecords.isEmpty) fs2.Stream.empty
            else {
              val enrichedRecords = childRecords.map { childRecord =>
                ParentChildUtils.enrichChildRecord(childRecord, parentKeyJson.get, partition.parentKeyColumn)
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
  }

  private def executeBatchJoin(
      parentRecords: fs2.Stream[IO, Json],
      session: RecordSession,
      arrowBatchSize: Int
  ): fs2.Stream[IO, InternalRow] = {
    val batchParam = partition.tableConfig.batchParam.getOrElse(
      throw new IllegalArgumentException(
        "Batch join strategy requires 'batch-param' in table config"
      )
    )
    val maxBatchSize = partition.tableConfig.batchSize
    val separator = partition.tableConfig.batchSeparator


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

        session.pages(ReadRequest(
          handleFor(partition.tableConfig.endpoint, withTableConfig = true),
          batchParams,
          partition.pushedLimit
        )).flatMap { page =>
          val childRecords = page.records
          if (childRecords.isEmpty) fs2.Stream.empty
          else {
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
                  List.empty
              }
            }

            if (enrichedRecords.isEmpty) fs2.Stream.empty
            else {
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
  }
}

package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.config.JoinStrategy
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
    val joinStrategy = partition.tableConfig.joinStrategy.getOrElse(JoinStrategy.NestedLoop)

    // Use effective rate limit calculated by ParentChildScan (distributed across partitions)
    val httpConfig = partition.effectiveRateLimit match {
      case Some(_) => partition.sourceConfig.http.copy(rateLimit = partition.effectiveRateLimit)
      case None    => partition.sourceConfig.http
    }

    val program: IO[List[InternalRow]] = Client
      .resource(httpConfig, partition.sourceConfig.auth)
      .use { client =>
        // Fetch parent records
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

        val rowsStream = joinStrategy match {
          case JoinStrategy.Batch =>
            executeBatchJoin(parentRecords, client, batchSize)
          case JoinStrategy.NestedLoop =>
            executeNestedLoopJoin(parentRecords, client, batchSize)
        }
        rowsStream.compile.toList
      }

    program.unsafeRunSync().iterator
  }

  private def executeNestedLoopJoin(
      parentRecords: fs2.Stream[IO, Json],
      client: Client.RestClient,
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
      client: Client.RestClient,
      arrowBatchSize: Int
  ): fs2.Stream[IO, InternalRow] = {
    val batchParam = partition.tableConfig.batchParam.getOrElse(
      throw new IllegalArgumentException(
        "Batch join strategy requires 'batch-param' in table config"
      )
    )
    val maxBatchSize = partition.tableConfig.batchSize
    val separator = partition.tableConfig.batchSeparator

    val childBaseUri = Uri.unsafeFromString(partition.baseUrl + partition.tableConfig.endpoint)
    val dataPath = partition.tableConfig.dataPath

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

        Paginator.pages(
          client,
          childBaseUri,
          batchParams,
          partition.sourceConfig.pagination,
          partition.pushedLimit
        ).flatMap { pageJson =>
          val childRecords = Converter.extractRecords(pageJson, dataPath)
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

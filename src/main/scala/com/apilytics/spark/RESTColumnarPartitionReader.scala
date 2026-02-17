package com.apilytics.spark

import cats.effect.{IO, Resource}
import com.apilytics.core.arrow.Converter
import com.apilytics.core.config.SchemaMode
import com.apilytics.core.http.{Client, Paginator, ResponseCache}
import fs2.Stream
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.http4s.Uri

/** Zero-copy columnar reader that lazily streams Arrow batches via a bounded queue.
  * Memory scales with batch size, not total partition size.
  *
  * For variant mode, bypasses Arrow entirely and uses native Spark VariantType
  * for efficient semi-structured data queries with path navigation syntax.
  */
class RESTColumnarPartitionReader(partition: RESTInputPartition) extends LazyColumnarReader {

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

  override protected def clientResource: Resource[IO, Client.RestClient] =
    Client.resource(httpConfig, partition.sourceConfig.auth, responseCache)

  override protected def buildStream(
      client: Client.RestClient
  ): Stream[IO, (org.apache.spark.sql.vectorized.ColumnarBatch, VectorSchemaRoot)] = {
    val baseUri = Uri.unsafeFromString(partition.baseUrl + partition.endpoint.path)
    val dataPath = partition.tableConfig.flatMap(_.dataPath)
    val batchSize = partition.sourceConfig.schema.arrowBatchSize

    Paginator
      .pages(client, baseUri, partition.pushedParams, partition.sourceConfig.pagination, partition.pushedLimit, partition.responseFormat)
      .flatMap { pageJson =>
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
}

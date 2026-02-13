package com.apilytics.spark

import cats.effect.{IO, Resource}
import com.apilytics.core.arrow.Converter
import com.apilytics.core.http.{Client, Paginator, ResponseCache}
import fs2.Stream
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.http4s.Uri

/** Zero-copy columnar reader that lazily streams Arrow batches via a bounded queue.
  * Memory scales with batch size, not total partition size.
  */
class RESTColumnarPartitionReader(partition: RESTInputPartition) extends LazyColumnarReader {

  override protected val allocator: RootAllocator = new RootAllocator()
  override protected val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  // Use singleton cache that persists across queries on this executor
  private val responseCache = ResponseCache.fromConfig(partition.sourceConfig.http.responseCache)

  // Use effective rate limit calculated by RESTScan (distributed across partitions)
  private val httpConfig = partition.effectiveRateLimit match {
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
      .pages(client, baseUri, partition.pushedParams, partition.sourceConfig.pagination, partition.pushedLimit)
      .flatMap { pageJson =>
        val records = Converter.extractRecords(pageJson, dataPath)
        if (records.isEmpty) Stream.empty
        else {
          val chunks = records.grouped(batchSize).toList
          Stream.emits(chunks.map { chunk =>
            val root = Converter.toArrow(chunk, arrowSchema, allocator)
            (arrowToBatch(root), root)
          })
        }
      }
  }
}

package com.apilytics.spark

import cats.effect.{IO, Resource}
import com.apilytics.core.arrow.Converter
import com.apilytics.core.http.{Client, Paginator, ResponseCache}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.http4s.Uri

/** Zero-copy columnar reader for exploded array views.
  * Each API record is exploded by the array field, then lazily streamed as Arrow batches.
  * Memory scales with batch size, not total partition size.
  */
class ExplodedArrayColumnarPartitionReader(partition: ExplodedArrayInputPartition)
    extends LazyColumnarReader {

  override protected val allocator: RootAllocator = new RootAllocator()
  override protected val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  // Use singleton cache that persists across queries on this executor
  private val responseCache = ResponseCache.fromConfig(partition.sourceConfig.http.responseCache)

  override protected def clientResource: Resource[IO, Client.RestClient] =
    Client.resource(partition.sourceConfig.http, partition.sourceConfig.auth, responseCache)

  override protected def buildStream(
      client: Client.RestClient
  ): fs2.Stream[IO, (ColumnarBatch, VectorSchemaRoot)] = {
    val baseUri = Uri.unsafeFromString(partition.baseUrl + partition.endpoint.path)
    val dataPath = partition.tableConfig.flatMap(_.dataPath)
    val batchSize = partition.sourceConfig.schema.arrowBatchSize
    val outer = partition.sourceConfig.schema.explodeOuter

    Paginator
      .pages(client, baseUri, partition.pushedParams, partition.sourceConfig.pagination, partition.pushedLimit)
      .map(_._1)
      .flatMap { pageJson =>
        val records = Converter.extractRecords(pageJson, dataPath)
        val exploded = records.flatMap(r =>
          ExplodedArrayOps.explodeRecord(r, partition.arrayFieldName, partition.arrayJsonPath, outer)
        )
        if (exploded.isEmpty) fs2.Stream.empty
        else {
          val chunks = exploded.grouped(batchSize).toList
          fs2.Stream.emits(chunks.map { chunk =>
            val root = Converter.toArrow(chunk, arrowSchema, allocator)
            (arrowToBatch(root), root)
          })
        }
      }
  }
}

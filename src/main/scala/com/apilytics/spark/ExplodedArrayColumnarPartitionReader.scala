package com.apilytics.spark

import cats.effect.IO
import com.apilytics.core.arrow.Converter
import com.apilytics.core.rest.{RestHandle, RestSource}
import com.apilytics.core.source.{ReadRequest, RecordSession, RecordSource}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Zero-copy columnar reader for exploded array views.
  * Each API record is exploded by the array field, then lazily streamed as Arrow batches.
  * Memory scales with batch size, not total partition size.
  */
class ExplodedArrayColumnarPartitionReader(partition: ExplodedArrayInputPartition)
    extends LazyColumnarReader {

  override protected val allocator: RootAllocator = new RootAllocator()
  override protected val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  override protected def prefetchSize: Int = partition.sourceConfig.schema.prefetchBatches

  override protected def recordSource: RecordSource = new RestSource(partition.sourceConfig)

  override protected def buildStream(
      session: RecordSession
  ): fs2.Stream[IO, (ColumnarBatch, VectorSchemaRoot)] = {
    val batchSize = partition.sourceConfig.schema.arrowBatchSize
    val outer = partition.sourceConfig.schema.explodeOuter
    val handle = RestHandle(partition.endpoint.path, partition.baseUrl, partition.tableConfig)

    session
      .pages(ReadRequest(handle, partition.pushedParams, partition.pushedLimit))
      .flatMap { page =>
        val exploded = page.records.flatMap(r =>
          ExplodedArrayOps.explodeRecord(r, partition.arrayFieldName, partition.arrayJsonPath, outer)
        )
        if (exploded.isEmpty) fs2.Stream.empty
        else {
          // Convert in `map`, not inside `emits`, so batches are allocated as the
          // bounded queue pulls them rather than all at once per page.
          fs2.Stream.emits(exploded.grouped(batchSize).toList).unchunk.map { chunk =>
            val root = Converter.toArrow(chunk, arrowSchema, allocator)
            (arrowToBatch(root), root)
          }
        }
      }
  }
}

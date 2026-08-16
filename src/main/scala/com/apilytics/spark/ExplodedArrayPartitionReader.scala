package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.rest.{RestHandle, RestSource}
import com.apilytics.core.source.ReadRequest
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

/** Row-based partition reader for exploded array views.
  *
  * NOTE: This reader eagerly materializes all rows into memory via `.compile.toList`.
  * It exists as a debug/test fallback only — production execution uses the columnar
  * reader (ExplodedArrayColumnarPartitionReader) which streams lazily via LazyColumnarReader.
  * Since supportColumnarReads always returns true, Spark will never instantiate this
  * reader in normal execution.
  */
class ExplodedArrayPartitionReader(partition: ExplodedArrayInputPartition) extends PartitionReader[InternalRow] {

  private val allocator = new RootAllocator()
  private val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  private val rows: Iterator[InternalRow] = fetchExplodedRows()
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

  private def fetchExplodedRows(): Iterator[InternalRow] = {
    val outer = partition.sourceConfig.schema.explodeOuter
    val handle = RestHandle(partition.endpoint.path, partition.baseUrl, partition.tableConfig)

    val program: IO[List[InternalRow]] = new RestSource(partition.sourceConfig).session
      .use { session =>
        session
          .pages(ReadRequest(handle, partition.pushedParams, partition.pushedLimit))
          .flatMap { page =>
            val exploded = page.records.flatMap(r =>
              ExplodedArrayOps.explodeRecord(r, partition.arrayFieldName, partition.arrayJsonPath, outer)
            )
            if (exploded.isEmpty) fs2.Stream.empty
            else {
              val root = Converter.toArrow(exploded, arrowSchema, allocator)
              val rows = try {
                ArrowUtils.arrowToInternalRows(root)
              } finally {
                root.close()
              }
              fs2.Stream.emits(rows)
            }
          }
          .compile
          .toList
      }

    program.unsafeRunSync().iterator
  }
}

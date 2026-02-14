package com.apilytics.spark

import com.apilytics.core.config.PaginationConfig
import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.types.StructType

import java.util.OptionalLong

/** Provides statistics estimation for REST API scans.
  *
  * Statistics help Spark choose better join strategies and memory allocation.
  * We only report statistics when we have real signal:
  * - Pushed LIMIT gives us an exact upper bound
  * - Without LIMIT, we return empty stats ("I don't know")
  *
  * Returning arbitrary estimates (like maxPageSize * maxPages) could cause Spark
  * to broadcast a table that triggers a full paginated fetch - worse than no stats.
  */
object ScanStatistics {

  /** Average bytes per field for size estimation. */
  private val AvgBytesPerField = 50L

  /** Create statistics based on available information.
    *
    * Only reports stats when we have actual signal (pushed LIMIT).
    * Returns empty stats otherwise to let Spark use conservative planning.
    *
    * @param pushedLimit Optional LIMIT clause value
    * @param paginationConfig Pagination configuration (unused, kept for API compatibility)
    * @param schema Schema for size estimation
    * @return Statistics with estimated numRows and sizeInBytes, or empty if unknown
    */
  def estimate(
      pushedLimit: Option[Int],
      @annotation.nowarn("cat=unused") paginationConfig: PaginationConfig,
      schema: StructType
  ): Statistics = {
    val estimatedRows = pushedLimit.map(_.toLong)
    val estimatedBytes = estimateSizeInBytes(estimatedRows, schema)

    new Statistics {
      override def sizeInBytes(): OptionalLong =
        estimatedBytes.map(OptionalLong.of).getOrElse(OptionalLong.empty())

      override def numRows(): OptionalLong =
        estimatedRows.map(OptionalLong.of).getOrElse(OptionalLong.empty())
    }
  }

  /** Estimate total size in bytes from row count and schema. */
  private def estimateSizeInBytes(rowCount: Option[Long], schema: StructType): Option[Long] = {
    rowCount.map { rows =>
      val avgRowSize = schema.fields.length * AvgBytesPerField
      rows * avgRowSize
    }
  }
}

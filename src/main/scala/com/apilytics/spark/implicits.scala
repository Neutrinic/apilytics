package com.apilytics.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Implicits for cleaner error handling with Apilytics.
  *
  * Usage:
  * {{{
  * import com.apilytics.spark.implicits._
  *
  * // SQL with clean errors
  * spark.api.sql("SELECT * FROM api.default.users").show()
  *
  * // Table with clean errors
  * spark.api.table("api.default.users").show()
  *
  * // Chain from existing DataFrame
  * spark.table("api.default.users").api.show()
  * }}}
  */
object implicits {

  /** Extension methods for SparkSession. */
  implicit class SparkSessionOps(spark: SparkSession) {
    /** Returns an ApiSparkSession wrapper with clean error handling. */
    def api: ApiSparkSession = new ApiSparkSession(spark)
  }

  /** Extension methods for DataFrame. */
  implicit class DataFrameOps(df: DataFrame) {
    /** Returns an ApiDataFrame wrapper with clean error handling. */
    def api: ApiDataFrame = new ApiDataFrame(df)
  }
}

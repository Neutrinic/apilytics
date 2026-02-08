package com.apilytics.spark

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/** Implicits for cleaner error handling with Apilytics.
  *
  * Usage:
  * {{{
  * import com.apilytics.spark.implicits._
  *
  * // Wrap any operation in withCleanErrors
  * withCleanErrors {
  *   spark.sql("SELECT * FROM api.default.issues").show()
  * }
  *
  * // Or use extension methods on DataFrame
  * spark.sql("SELECT * FROM api.default.issues").showClean()
  * spark.table("api.default.issues").showClean(20)
  * }}}
  */
object implicits {

  /** Wrapper for clean error handling - catches Spark exceptions and reformats them. */
  def withCleanErrors[T](block: => T): T = ErrorHandler.withCleanErrors(block)

  /** Extension methods for DataFrame with clean error handling on terminal actions. */
  implicit class DataFrameCleanOps(df: DataFrame) {

    /** Show the DataFrame with clean error handling. */
    def showClean(): Unit = ErrorHandler.withCleanErrors { df.show() }

    /** Show the DataFrame with clean error handling. */
    def showClean(numRows: Int): Unit = ErrorHandler.withCleanErrors { df.show(numRows) }

    /** Show the DataFrame with clean error handling. */
    def showClean(truncate: Boolean): Unit = ErrorHandler.withCleanErrors { df.show(truncate) }

    /** Show the DataFrame with clean error handling. */
    def showClean(numRows: Int, truncate: Boolean): Unit =
      ErrorHandler.withCleanErrors { df.show(numRows, truncate) }

    /** Show the DataFrame with clean error handling. */
    def showClean(numRows: Int, truncate: Int): Unit =
      ErrorHandler.withCleanErrors { df.show(numRows, truncate) }

    /** Show the DataFrame with clean error handling. */
    def showClean(numRows: Int, truncate: Int, vertical: Boolean): Unit =
      ErrorHandler.withCleanErrors { df.show(numRows, truncate, vertical) }

    /** Collect all rows with clean error handling. */
    def collectClean(): Array[Row] = ErrorHandler.withCleanErrors { df.collect() }

    /** Count rows with clean error handling. */
    def countClean(): Long = ErrorHandler.withCleanErrors { df.count() }

    /** Take first n rows with clean error handling. */
    def takeClean(n: Int): Array[Row] = ErrorHandler.withCleanErrors { df.take(n) }
  }

  /** Extension methods for SparkSession. */
  implicit class SparkSessionCleanOps(spark: SparkSession) {

    /** Execute SQL with clean error handling. */
    def sqlClean(sqlText: String): DataFrame =
      ErrorHandler.withCleanErrors { spark.sql(sqlText) }
  }
}

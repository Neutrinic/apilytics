package com.apilytics.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Wrapper around SparkSession that provides clean error handling for Apilytics operations.
  *
  * Instead of Spark's verbose stack traces, errors are reformatted to be concise and actionable.
  */
class ApiSparkSession(spark: SparkSession) {

  /** Execute a SQL query with clean error handling.
    *
    * @param sqlText The SQL query to execute
    * @return DataFrame result, or throws a clean ApiException on error
    */
  def sql(sqlText: String): ApiDataFrame = {
    new ApiDataFrame(ErrorHandler.withCleanErrors {
      spark.sql(sqlText)
    })
  }

  /** Load a table with clean error handling.
    *
    * @param tableName Fully qualified table name (e.g., "api.default.users")
    * @return ApiDataFrame wrapper for the table
    */
  def table(tableName: String): ApiDataFrame = {
    new ApiDataFrame(ErrorHandler.withCleanErrors {
      spark.table(tableName)
    })
  }

  /** Access the underlying SparkSession. */
  def underlying: SparkSession = spark
}

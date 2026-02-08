package com.apilytics.spark

import org.apache.spark.sql.{DataFrame, Row}

/** Wrapper around DataFrame that provides clean error handling for terminal actions.
  *
  * All DataFrame transformations are passed through, but terminal actions (show, collect, etc.)
  * are wrapped with error handling to provide clean, actionable error messages.
  */
class ApiDataFrame(df: DataFrame) {

  // ============================================================================
  // Terminal Actions (with error handling)
  // ============================================================================

  /** Show the DataFrame with clean error handling. */
  def show(): Unit = ErrorHandler.withCleanErrors { df.show() }

  /** Show the DataFrame with clean error handling. */
  def show(numRows: Int): Unit = ErrorHandler.withCleanErrors { df.show(numRows) }

  /** Show the DataFrame with clean error handling. */
  def show(truncate: Boolean): Unit = ErrorHandler.withCleanErrors { df.show(truncate) }

  /** Show the DataFrame with clean error handling. */
  def show(numRows: Int, truncate: Boolean): Unit = ErrorHandler.withCleanErrors { df.show(numRows, truncate) }

  /** Show the DataFrame with clean error handling. */
  def show(numRows: Int, truncate: Int): Unit = ErrorHandler.withCleanErrors { df.show(numRows, truncate) }

  /** Show the DataFrame with clean error handling. */
  def show(numRows: Int, truncate: Int, vertical: Boolean): Unit =
    ErrorHandler.withCleanErrors { df.show(numRows, truncate, vertical) }

  /** Collect all rows with clean error handling. */
  def collect(): Array[Row] = ErrorHandler.withCleanErrors { df.collect() }

  /** Count rows with clean error handling. */
  def count(): Long = ErrorHandler.withCleanErrors { df.count() }

  /** Take first n rows with clean error handling. */
  def take(n: Int): Array[Row] = ErrorHandler.withCleanErrors { df.take(n) }

  /** Get first row with clean error handling. */
  def first(): Row = ErrorHandler.withCleanErrors { df.first() }

  /** Get first row as Option with clean error handling. */
  def head(): Row = ErrorHandler.withCleanErrors { df.head() }

  /** Get first n rows with clean error handling. */
  def head(n: Int): Array[Row] = ErrorHandler.withCleanErrors { df.head(n) }

  /** Iterate over rows with clean error handling. */
  def foreach(f: Row => Unit): Unit = ErrorHandler.withCleanErrors { df.foreach(f) }

  /** Iterate over partitions with clean error handling. */
  def foreachPartition(f: Iterator[Row] => Unit): Unit =
    ErrorHandler.withCleanErrors { df.foreachPartition(f) }

  // ============================================================================
  // Transformations (with error handling, return ApiDataFrame)
  // ============================================================================

  def select(cols: String*): ApiDataFrame =
    new ApiDataFrame(ErrorHandler.withCleanErrors { df.select(cols.head, cols.tail: _*) })
  def selectExpr(exprs: String*): ApiDataFrame =
    new ApiDataFrame(ErrorHandler.withCleanErrors { df.selectExpr(exprs: _*) })
  def filter(condition: String): ApiDataFrame =
    new ApiDataFrame(ErrorHandler.withCleanErrors { df.filter(condition) })
  def where(condition: String): ApiDataFrame =
    new ApiDataFrame(ErrorHandler.withCleanErrors { df.where(condition) })
  def limit(n: Int): ApiDataFrame = new ApiDataFrame(df.limit(n))
  def orderBy(sortCol: String, sortCols: String*): ApiDataFrame =
    new ApiDataFrame(ErrorHandler.withCleanErrors { df.orderBy(sortCol, sortCols: _*) })
  def groupBy(cols: String*): ApiGroupedData =
    new ApiGroupedData(ErrorHandler.withCleanErrors { df.groupBy(cols.head, cols.tail: _*) })
  def distinct(): ApiDataFrame = new ApiDataFrame(df.distinct())
  def drop(colNames: String*): ApiDataFrame =
    new ApiDataFrame(ErrorHandler.withCleanErrors { df.drop(colNames: _*) })
  def dropDuplicates(): ApiDataFrame = new ApiDataFrame(df.dropDuplicates())
  def dropDuplicates(colNames: Seq[String]): ApiDataFrame = new ApiDataFrame(df.dropDuplicates(colNames))

  // ============================================================================
  // Schema & Metadata (pass-through)
  // ============================================================================

  def schema = df.schema
  def columns = df.columns
  def dtypes = df.dtypes
  def printSchema(): Unit = df.printSchema()
  def explain(): Unit = df.explain()
  def explain(extended: Boolean): Unit = df.explain(extended)

  // ============================================================================
  // Access underlying DataFrame
  // ============================================================================

  /** Access the underlying Spark DataFrame for operations not wrapped here. */
  def underlying: DataFrame = df

  /** Alias for underlying - allows `df.api.toDF` to get back to regular DataFrame. */
  def toDF: DataFrame = df
}

/** Wrapper for grouped data. */
class ApiGroupedData(grouped: org.apache.spark.sql.RelationalGroupedDataset) {
  def count(): ApiDataFrame = new ApiDataFrame(grouped.count())
  def sum(colNames: String*): ApiDataFrame = new ApiDataFrame(grouped.sum(colNames: _*))
  def avg(colNames: String*): ApiDataFrame = new ApiDataFrame(grouped.avg(colNames: _*))
  def min(colNames: String*): ApiDataFrame = new ApiDataFrame(grouped.min(colNames: _*))
  def max(colNames: String*): ApiDataFrame = new ApiDataFrame(grouped.max(colNames: _*))
  def mean(colNames: String*): ApiDataFrame = new ApiDataFrame(grouped.mean(colNames: _*))
}

package com.apilytics.spark

import io.circe.Json
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Builder for creating ColumnarBatch with native VARIANT type.
 *
 * This bypasses Arrow entirely, directly converting JSON records
 * to Spark's binary VARIANT format for efficient semi-structured queries.
 */
object VariantBatch {

  /**
   * Create a ColumnarBatch containing a single VARIANT column.
   *
   * @param records List of JSON records (each becomes one VARIANT value)
   * @return ColumnarBatch with single "value" column of VariantType
   */
  def fromRecords(records: List[Json]): ColumnarBatch = {
    val vector = new VariantColumnVector(records.toArray)
    val batch = new ColumnarBatch(Array(vector))
    batch.setNumRows(records.size)
    batch
  }
}

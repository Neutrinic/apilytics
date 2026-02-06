package com.apilytics.spark

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.unsafe.types.UTF8String

import scala.jdk.CollectionConverters._

/** Shared utilities for Arrow to Spark conversions. */
object ArrowUtils {

  /** Convert an Arrow VectorSchemaRoot to a list of Spark InternalRows.
    *
    * Used by row-based partition readers (RESTPartitionReader, ExplodedArrayPartitionReader,
    * ParentChildPartitionReader) as a fallback when columnar reads are not used.
    */
  def arrowToInternalRows(root: VectorSchemaRoot): List[InternalRow] = {
    val fieldCount = root.getFieldVectors.size()
    val rowCount = root.getRowCount

    (0 until rowCount).map { rowIdx =>
      val values = new Array[Any](fieldCount)
      root.getFieldVectors.asScala.zipWithIndex.foreach { case (vector, colIdx) =>
        values(colIdx) = if (vector.isNull(rowIdx)) {
          null
        } else {
          vector match {
            case v: org.apache.arrow.vector.VarCharVector =>
              UTF8String.fromBytes(v.get(rowIdx))
            case v: org.apache.arrow.vector.IntVector =>
              v.get(rowIdx)
            case v: org.apache.arrow.vector.BigIntVector =>
              v.get(rowIdx)
            case v: org.apache.arrow.vector.Float8Vector =>
              v.get(rowIdx)
            case v: org.apache.arrow.vector.BitVector =>
              v.get(rowIdx) == 1
            case v: org.apache.arrow.vector.DateDayVector =>
              v.get(rowIdx) // days since epoch, Spark DateType uses int
            case v: org.apache.arrow.vector.TimeStampMicroTZVector =>
              v.get(rowIdx) // micros since epoch, Spark TimestampType uses long
            case _ => null
          }
        }
      }
      new GenericInternalRow(values)
    }.toList
  }
}

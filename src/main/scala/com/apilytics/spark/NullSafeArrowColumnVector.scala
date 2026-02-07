package com.apilytics.spark

import org.apache.arrow.vector._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.types.UTF8String

/**
 * Null-safe wrapper around Arrow FieldVector for Spark's ColumnVector interface.
 *
 * Spark's ArrowColumnVector delegates directly to Arrow's vector.get() methods,
 * which throw IllegalStateException when accessing null values. This wrapper
 * checks isNull() before accessing values, returning appropriate defaults for nulls.
 */
class NullSafeArrowColumnVector(vector: FieldVector)
    extends ColumnVector(NullSafeArrowColumnVector.toSparkType(vector)) {

  override def close(): Unit = {
    // Don't close the vector - it's owned by VectorSchemaRoot and will be closed there
  }

  override def hasNull: Boolean = vector.getNullCount > 0

  override def numNulls(): Int = vector.getNullCount

  override def isNullAt(rowId: Int): Boolean = vector.isNull(rowId)

  override def getBoolean(rowId: Int): Boolean = {
    if (vector.isNull(rowId)) false
    else vector.asInstanceOf[BitVector].get(rowId) == 1
  }

  override def getByte(rowId: Int): Byte = {
    if (vector.isNull(rowId)) 0
    else vector.asInstanceOf[TinyIntVector].get(rowId)
  }

  override def getShort(rowId: Int): Short = {
    if (vector.isNull(rowId)) 0
    else vector.asInstanceOf[SmallIntVector].get(rowId)
  }

  override def getInt(rowId: Int): Int = {
    if (vector.isNull(rowId)) 0
    else vector match {
      case v: IntVector => v.get(rowId)
      case v: DateDayVector => v.get(rowId)
      case _ => 0
    }
  }

  override def getLong(rowId: Int): Long = {
    if (vector.isNull(rowId)) 0L
    else vector match {
      case v: BigIntVector => v.get(rowId)
      case v: TimeStampVector => v.get(rowId)
      case _ => 0L
    }
  }

  override def getFloat(rowId: Int): Float = {
    if (vector.isNull(rowId)) 0f
    else vector.asInstanceOf[Float4Vector].get(rowId)
  }

  override def getDouble(rowId: Int): Double = {
    if (vector.isNull(rowId)) 0d
    else vector.asInstanceOf[Float8Vector].get(rowId)
  }

  override def getDecimal(rowId: Int, precision: Int, scale: Int): org.apache.spark.sql.types.Decimal = {
    if (vector.isNull(rowId)) Decimal(0)
    else {
      val v = vector.asInstanceOf[DecimalVector]
      val bd = v.getObject(rowId)
      Decimal.apply(bd, precision, scale)
    }
  }

  // Note: We return EMPTY_UTF8/empty array for nulls instead of null because Spark's
  // codegen (UnsafeWriter) can call getters without checking isNullAt() first, causing NPE.
  // Spark still correctly identifies nulls via isNullAt() which we implement properly.
  override def getUTF8String(rowId: Int): UTF8String = {
    if (vector.isNull(rowId)) UTF8String.EMPTY_UTF8
    else {
      val v = vector.asInstanceOf[VarCharVector]
      val bytes = v.get(rowId)
      UTF8String.fromBytes(bytes)
    }
  }

  override def getBinary(rowId: Int): Array[Byte] = {
    if (vector.isNull(rowId)) Array.emptyByteArray
    else vector.asInstanceOf[VarBinaryVector].get(rowId)
  }

  override def getArray(rowId: Int): org.apache.spark.sql.vectorized.ColumnarArray = {
    throw new UnsupportedOperationException("Array type not supported")
  }

  override def getMap(rowId: Int): org.apache.spark.sql.vectorized.ColumnarMap = {
    throw new UnsupportedOperationException("Map type not supported")
  }

  override def getChild(ordinal: Int): ColumnVector = {
    throw new UnsupportedOperationException("Struct type not supported")
  }
}

object NullSafeArrowColumnVector {
  def apply(vector: FieldVector): NullSafeArrowColumnVector = new NullSafeArrowColumnVector(vector)

  private[spark] def toSparkType(vector: FieldVector): DataType = vector match {
    case _: BitVector => BooleanType
    case _: TinyIntVector => ByteType
    case _: SmallIntVector => ShortType
    case _: IntVector => IntegerType
    case _: BigIntVector => LongType
    case _: Float4Vector => FloatType
    case _: Float8Vector => DoubleType
    case _: VarCharVector => StringType
    case _: VarBinaryVector => BinaryType
    case _: DateDayVector => DateType
    case _: TimeStampVector => TimestampType
    case _: DecimalVector => DecimalType.SYSTEM_DEFAULT
    case _ => StringType
  }
}

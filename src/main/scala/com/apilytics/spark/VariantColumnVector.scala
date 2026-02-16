package com.apilytics.spark

import io.circe.Json
import org.apache.spark.sql.types.{BinaryType, VariantType}
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.types.variant.{Variant, VariantBuilder}
import org.apache.spark.unsafe.types.UTF8String

/**
 * ColumnVector implementation for Spark's native VARIANT type.
 *
 * This bypasses Arrow entirely for the variant path, directly converting
 * JSON records to Spark's binary VARIANT format using VariantBuilder.
 * This enables native path navigation syntax (value:field::type) in Spark SQL.
 *
 * The ColumnVector.getVariant() method is final and calls:
 *   new VariantVal(getChild(0).getBinary(rowId), getChild(1).getBinary(rowId))
 *
 * So we implement getChild() to return two binary child vectors:
 *   - child 0: variant value bytes
 *   - child 1: variant metadata bytes
 *
 * @param records List of JSON records to expose as VARIANT values
 */
class VariantColumnVector(records: Array[Json]) extends ColumnVector(VariantType) {

  // Parse JSON to VARIANT binary format
  private val parsedVariants: Array[Variant] = records.map { json =>
    if (json.isNull) null
    else VariantBuilder.parseJson(json.noSpaces, false)
  }

  // Child vectors for value and metadata bytes
  private val valueChild = new BinaryChildVector(parsedVariants.map(v => if (v == null) null else v.getValue))
  private val metadataChild = new BinaryChildVector(parsedVariants.map(v => if (v == null) null else v.getMetadata))

  override def close(): Unit = {
    valueChild.close()
    metadataChild.close()
  }

  override def hasNull: Boolean = parsedVariants.exists(_ == null)

  override def numNulls(): Int = parsedVariants.count(_ == null)

  override def isNullAt(rowId: Int): Boolean = parsedVariants(rowId) == null

  // getChild is called by the final getVariant() method
  override def getChild(ordinal: Int): ColumnVector = ordinal match {
    case 0 => valueChild
    case 1 => metadataChild
    case _ => throw new IndexOutOfBoundsException(s"Variant has 2 children, got ordinal $ordinal")
  }

  // The following methods are not applicable for VariantType but must be implemented

  override def getBoolean(rowId: Int): Boolean =
    throw new UnsupportedOperationException("VariantType does not support getBoolean")

  override def getByte(rowId: Int): Byte =
    throw new UnsupportedOperationException("VariantType does not support getByte")

  override def getShort(rowId: Int): Short =
    throw new UnsupportedOperationException("VariantType does not support getShort")

  override def getInt(rowId: Int): Int =
    throw new UnsupportedOperationException("VariantType does not support getInt")

  override def getLong(rowId: Int): Long =
    throw new UnsupportedOperationException("VariantType does not support getLong")

  override def getFloat(rowId: Int): Float =
    throw new UnsupportedOperationException("VariantType does not support getFloat")

  override def getDouble(rowId: Int): Double =
    throw new UnsupportedOperationException("VariantType does not support getDouble")

  override def getDecimal(rowId: Int, precision: Int, scale: Int): org.apache.spark.sql.types.Decimal =
    throw new UnsupportedOperationException("VariantType does not support getDecimal")

  override def getUTF8String(rowId: Int): UTF8String =
    throw new UnsupportedOperationException("VariantType does not support getUTF8String")

  override def getBinary(rowId: Int): Array[Byte] =
    throw new UnsupportedOperationException("VariantType does not support getBinary")

  override def getArray(rowId: Int): org.apache.spark.sql.vectorized.ColumnarArray =
    throw new UnsupportedOperationException("VariantType does not support getArray")

  override def getMap(rowId: Int): org.apache.spark.sql.vectorized.ColumnarMap =
    throw new UnsupportedOperationException("VariantType does not support getMap")
}

/**
 * Simple binary child vector for Variant's value/metadata bytes.
 */
private class BinaryChildVector(data: Array[Array[Byte]]) extends ColumnVector(BinaryType) {

  override def close(): Unit = {}

  override def hasNull: Boolean = data.exists(_ == null)

  override def numNulls(): Int = data.count(_ == null)

  override def isNullAt(rowId: Int): Boolean = data(rowId) == null

  override def getBinary(rowId: Int): Array[Byte] = {
    if (data(rowId) == null) Array.emptyByteArray
    else data(rowId)
  }

  // Not applicable for BinaryType child

  override def getBoolean(rowId: Int): Boolean =
    throw new UnsupportedOperationException("BinaryType does not support getBoolean")

  override def getByte(rowId: Int): Byte =
    throw new UnsupportedOperationException("BinaryType does not support getByte")

  override def getShort(rowId: Int): Short =
    throw new UnsupportedOperationException("BinaryType does not support getShort")

  override def getInt(rowId: Int): Int =
    throw new UnsupportedOperationException("BinaryType does not support getInt")

  override def getLong(rowId: Int): Long =
    throw new UnsupportedOperationException("BinaryType does not support getLong")

  override def getFloat(rowId: Int): Float =
    throw new UnsupportedOperationException("BinaryType does not support getFloat")

  override def getDouble(rowId: Int): Double =
    throw new UnsupportedOperationException("BinaryType does not support getDouble")

  override def getDecimal(rowId: Int, precision: Int, scale: Int): org.apache.spark.sql.types.Decimal =
    throw new UnsupportedOperationException("BinaryType does not support getDecimal")

  override def getUTF8String(rowId: Int): UTF8String =
    throw new UnsupportedOperationException("BinaryType does not support getUTF8String")

  override def getArray(rowId: Int): org.apache.spark.sql.vectorized.ColumnarArray =
    throw new UnsupportedOperationException("BinaryType does not support getArray")

  override def getMap(rowId: Int): org.apache.spark.sql.vectorized.ColumnarMap =
    throw new UnsupportedOperationException("BinaryType does not support getMap")

  override def getChild(ordinal: Int): ColumnVector =
    throw new UnsupportedOperationException("BinaryType does not support getChild")
}

object VariantColumnVector {
  /** Column name used for variant mode tables */
  val ColumnName = "value"
}

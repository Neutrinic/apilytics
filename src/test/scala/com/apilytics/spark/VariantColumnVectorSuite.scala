package com.apilytics.spark

import io.circe.Json
import io.circe.parser._
import munit.FunSuite
import org.apache.spark.types.variant.VariantBuilder

class VariantColumnVectorSuite extends FunSuite {

  test("VariantColumnVector round-trips JSON through Variant binary format") {
    val json = parse("""{"name": "Alice", "age": 30, "active": true}""").toOption.get
    val vector = new VariantColumnVector(Array(json))

    // getChild(0) = value bytes, getChild(1) = metadata bytes
    val valueBytes = vector.getChild(0).getBinary(0)
    val metadataBytes = vector.getChild(1).getBinary(0)

    // Reconstruct using Spark's Variant and verify round-trip
    val variant = new org.apache.spark.types.variant.Variant(valueBytes, metadataBytes)
    val roundTripped = variant.toJson(java.time.ZoneOffset.UTC)

    // Parse both and compare (order may differ)
    val original = parse(json.noSpaces).toOption.get
    val restored = parse(roundTripped).toOption.get
    assertEquals(restored, original)
  }

  test("VariantColumnVector handles null values") {
    val records = Array(Json.Null, parse("""{"x": 1}""").toOption.get, Json.Null)
    val vector = new VariantColumnVector(records)

    assert(vector.isNullAt(0))
    assert(!vector.isNullAt(1))
    assert(vector.isNullAt(2))
    assertEquals(vector.numNulls(), 2)
    assert(vector.hasNull)
  }

  test("VariantColumnVector.getVariant returns null for null rows") {
    // Spark's ColumnVector.getVariant() checks isNullAt() before calling getChild().getBinary()
    // This test verifies the contract works end-to-end
    val records = Array(Json.Null, parse("""{"x": 1}""").toOption.get, Json.Null)
    val vector = new VariantColumnVector(records)

    // getVariant() should return null for null rows (Spark checks isNullAt first)
    assert(vector.getVariant(0) == null)
    assert(vector.getVariant(2) == null)

    // Non-null row should return valid VariantVal
    val variant = vector.getVariant(1)
    assert(variant != null)
    // VariantVal wraps the binary - reconstruct Variant to get JSON
    val v = new org.apache.spark.types.variant.Variant(variant.getValue, variant.getMetadata)
    assertEquals(v.toJson(java.time.ZoneOffset.UTC), """{"x":1}""")
  }

  test("VariantColumnVector handles array values") {
    val json = parse("""[1, 2, 3]""").toOption.get
    val vector = new VariantColumnVector(Array(json))

    val valueBytes = vector.getChild(0).getBinary(0)
    val metadataBytes = vector.getChild(1).getBinary(0)
    val variant = new org.apache.spark.types.variant.Variant(valueBytes, metadataBytes)

    assertEquals(variant.toJson(java.time.ZoneOffset.UTC), "[1,2,3]")
  }

  test("VariantColumnVector handles nested objects") {
    val json = parse("""{"user": {"name": "Bob", "address": {"city": "NYC"}}}""").toOption.get
    val vector = new VariantColumnVector(Array(json))

    val valueBytes = vector.getChild(0).getBinary(0)
    val metadataBytes = vector.getChild(1).getBinary(0)
    val variant = new org.apache.spark.types.variant.Variant(valueBytes, metadataBytes)
    val roundTripped = variant.toJson(java.time.ZoneOffset.UTC)

    val restored = parse(roundTripped).toOption.get
    assertEquals(restored, json)
  }

  test("VariantColumnVector handles primitive values") {
    val records = Array(
      Json.fromString("hello"),
      Json.fromInt(42),
      Json.fromBoolean(true),
      Json.fromDoubleOrNull(3.14)
    )
    val vector = new VariantColumnVector(records)

    // Verify all are non-null
    assertEquals(vector.numNulls(), 0)

    // Verify each round-trips correctly
    for (i <- records.indices) {
      val valueBytes = vector.getChild(0).getBinary(i)
      val metadataBytes = vector.getChild(1).getBinary(i)
      val variant = new org.apache.spark.types.variant.Variant(valueBytes, metadataBytes)
      val json = variant.toJson(java.time.ZoneOffset.UTC)
      val restored = parse(json).toOption.get
      assertEquals(restored, records(i))
    }
  }

  test("VariantColumnVector close is safe to call multiple times") {
    val json = parse("""{"x": 1}""").toOption.get
    val vector = new VariantColumnVector(Array(json))

    // Should not throw
    vector.close()
    vector.close()
  }
}

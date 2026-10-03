package com.apilytics.core.arrow

import com.apilytics.core.schema.SourceSchema
import com.apilytics.core.schema.SchemaMapper
import io.circe.Json
import io.circe.parser._
import munit.FunSuite
import org.apache.arrow.memory.RootAllocator

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

class ConverterSuite extends FunSuite {

  private val allocator = new RootAllocator()

  override def afterAll(): Unit = allocator.close()

  test("extractRecords with no dataPath returns top-level array") {
    val json = parse("""[{"id": 1}, {"id": 2}]""").toOption.get
    val records = Converter.extractRecords(json, None)
    assertEquals(records.size, 2)
  }

  test("extractRecords with no dataPath and single object returns it as list") {
    val json = parse("""{"id": 1}""").toOption.get
    val records = Converter.extractRecords(json, None)
    assertEquals(records.size, 1)
  }

  test("extractRecords with dataPath extracts nested array") {
    val json = parse("""{"data": [{"id": 1}, {"id": 2}], "meta": {}}""").toOption.get
    val records = Converter.extractRecords(json, Some("/data"))
    assertEquals(records.size, 2)
  }

  test("extractRecords with invalid pointer throws") {
    val json = Json.obj()
    intercept[IllegalArgumentException] {
      Converter.extractRecords(json, Some("not a pointer"))
    }
  }

  test("extractRecords with missing path returns empty") {
    val json = parse("""{"other": [1, 2]}""").toOption.get
    val records = Converter.extractRecords(json, Some("/data"))
    assertEquals(records.size, 0)
  }

  test("toArrow converts string fields") {
    val schema = SchemaMapper.toArrowSchema(
      SourceSchema.ObjectType(Map("name" -> SourceSchema.StringType()), Set("name"))
    )
    val records = List(
      parse("""{"name": "Alice"}""").toOption.get,
      parse("""{"name": "Bob"}""").toOption.get
    )

    val root = Converter.toArrow(records, schema, allocator)
    try {
      assertEquals(root.getRowCount, 2)
      val vec = root.getVector("name").asInstanceOf[org.apache.arrow.vector.VarCharVector]
      assertEquals(new String(vec.get(0), StandardCharsets.UTF_8), "Alice")
      assertEquals(new String(vec.get(1), StandardCharsets.UTF_8), "Bob")
    } finally root.close()
  }

  test("toArrow converts integer fields") {
    val schema = SchemaMapper.toArrowSchema(
      SourceSchema.ObjectType(Map("count" -> SourceSchema.IntegerType()))
    )
    val records = List(parse("""{"count": 42}""").toOption.get)

    val root = Converter.toArrow(records, schema, allocator)
    try {
      val vec = root.getVector("count").asInstanceOf[org.apache.arrow.vector.BigIntVector]
      assertEquals(vec.get(0), 42L)
    } finally root.close()
  }

  /** Convert `values`, each the JSON for one record's `v`, into a column of type `t`. */
  private def readColumn(t: SourceSchema, values: String*): (List[Option[Any]], ConversionStats, List[String]) = {
    val schema   = SchemaMapper.toArrowSchema(SourceSchema.ObjectType(Map("v" -> t)))
    val warnings = scala.collection.mutable.ListBuffer.empty[String]
    val stats    = new ConversionStats(warnings += _)
    val records  = values.toList.map(v => parse(s"""{"v": $v}""").toOption.get)
    val root     = Converter.toArrow(records, schema, allocator, stats)
    try {
      val vec = root.getVector("v")
      (values.indices.map(i => Option(vec.getObject(i): Any)).toList, stats, warnings.toList)
    } finally root.close()
  }

  test("an integer without a format holds values past 32 bits (#308)") {
    val (read, stats, _) = readColumn(SourceSchema.IntegerType(), "3000000000", "1700000000000")
    assertEquals(read, List(Some(3000000000L), Some(1700000000000L)))
    assertEquals((stats.converted, stats.nulled), (0L, 0L))
  }

  test("an explicit int32 that overflows is NULL, counted and logged once (#308)") {
    val (read, stats, warnings) = readColumn(SourceSchema.IntegerType(Some("int32")), "7", "3000000000", "4000000000")
    assertEquals(read, List(Some(7), None, None))
    assertEquals(stats.nulled, 2L)
    assertEquals(warnings.size, 1, "one warning per column, not per value")
    assert(warnings.head.contains("'v'") && warnings.head.contains("3000000000"), warnings.head)
  }

  test("an integer column converts integer strings, and nothing fractional (#309)") {
    val (read, stats, _) =
      readColumn(SourceSchema.IntegerType(), "\"42\"", "\"-7\"", "\"3.0\"", "\"4.2\"", "4.2", "true", "\"\"", "3.0")
    assertEquals(read, List(Some(42L), Some(-7L), None, None, None, None, None, Some(3L)))
    assertEquals((stats.converted, stats.nulled), (2L, 5L))
  }

  test("a number column converts numeric strings (#309)") {
    val (read, stats, _) = readColumn(SourceSchema.NumberType(), "\"3.5\"", "\"1e3\"", "\"-.5\"", "2", "\"abc\"", "\"NaN\"")
    assertEquals(read, List(Some(3.5), Some(1000.0), Some(-0.5), Some(2.0), None, None))
    assertEquals((stats.converted, stats.nulled), (3L, 2L))
  }

  test("a boolean column converts true and false in any case, nothing else (#309)") {
    val (read, stats, _) = readColumn(SourceSchema.BooleanType, "\"TRUE\"", "\"false\"", "true", "\"yes\"", "\"1\"", "1")
    assertEquals(read, List(Some(true), Some(false), Some(true), None, None, None))
    assertEquals((stats.converted, stats.nulled), (2L, 3L))
  }

  test("a malformed date is NULL rather than failing the read (#309)") {
    val (read, stats, warnings) = readColumn(SourceSchema.StringType(Some("date")), "\"2024-01-02\"", "\"\"", "\"2024-13-01\"")
    assertEquals(read, List(Some(java.time.LocalDate.parse("2024-01-02").toEpochDay.toInt), None, None))
    assertEquals((stats.converted, stats.nulled), (0L, 2L))
    assertEquals(warnings.size, 1)
  }

  test("a timestamp column reads a space for T and a missing zone as UTC, and no epoch numbers (#309)") {
    def micros(s: String) = { val i = java.time.Instant.parse(s); i.getEpochSecond * 1000000 + i.getNano / 1000 }
    val (read, stats, _) = readColumn(
      SourceSchema.StringType(Some("date-time")),
      "\"2024-01-02T03:04:05Z\"",
      "\"2024-01-02T05:04:05+02:00\"",
      "\"2024-01-02 03:04:05Z\"",
      "\"2024-01-02T03:04:05\"",
      "\"2024-01-02 03:04:05.5\"",
      "1704164645",
      "\"\"",
      "\"yesterday\""
    )
    val t = micros("2024-01-02T03:04:05Z")
    assertEquals(read, List(Some(t), Some(t), Some(t), Some(t), Some(t + 500000), None, None, None))
    assertEquals((stats.converted, stats.nulled), (3L, 3L))
  }

  test("values outside what Spark can hold are NULL, not wrapped or infinite (#309)") {
    val (doubles, ds, _) = readColumn(SourceSchema.NumberType(), "\"1e309\"", "1e309", "\"1e308\"")
    assertEquals(doubles, List(None, None, Some(1e308)))
    assertEquals((ds.converted, ds.nulled), (1L, 2L))

    val (dates, dts, _) = readColumn(SourceSchema.StringType(Some("date")), "\"+10000-01-01\"", "\"9999-12-31\"")
    assertEquals(dates, List(None, Some(java.time.LocalDate.of(9999, 12, 31).toEpochDay.toInt)))
    assertEquals(dts.nulled, 1L)

    val (stamps, ts, _) = readColumn(
      SourceSchema.StringType(Some("date-time")),
      "\"+999999999-01-01T00:00:00Z\"", "\"+10000-01-01T00:00:00Z\"", "\"9999-12-31T23:59:59Z\""
    )
    assertEquals(stamps.take(2), List(None, None))
    assert(stamps(2).isDefined)
    assertEquals(ts.nulled, 2L)
  }

  test("a batch that fails partway is closed, not leaked (#307)") {
    // Four columns, and room for about two: allocating the third fails after the first two
    // hold memory. Unclosed, that memory made closing the reader fail with a leak on top of
    // the real error.
    val schema = SchemaMapper.toArrowSchema(SourceSchema.ObjectType(
      (1 to 4).map(i => s"c$i" -> SourceSchema.StringType()).toMap
    ))
    val records = List(parse("""{"c1": "a", "c2": "b", "c3": "c", "c4": "d"}""").toOption.get)
    val full = {
      val probe = allocator.newChildAllocator("probe", 0, Long.MaxValue)
      try { Converter.toArrow(records, schema, probe).close(); probe.getPeakMemoryAllocation }
      finally probe.close()
    }

    val limited = allocator.newChildAllocator("limited", 0, full / 2)
    try {
      intercept[org.apache.arrow.memory.OutOfMemoryException](Converter.toArrow(records, schema, limited))
      assertEquals(limited.getAllocatedMemory, 0L)
    } finally limited.close()
  }

  test("a JSON null or a missing field is NULL without counting (#309)") {
    val (read, stats, warnings) = readColumn(SourceSchema.IntegerType(), "null")
    assertEquals(read, List(None))
    assertEquals((stats.converted, stats.nulled, warnings), (0L, 0L, Nil))
  }

  test("toArrow converts boolean fields") {
    val schema = SchemaMapper.toArrowSchema(
      SourceSchema.ObjectType(Map("active" -> SourceSchema.BooleanType))
    )
    val records = List(
      parse("""{"active": true}""").toOption.get,
      parse("""{"active": false}""").toOption.get
    )

    val root = Converter.toArrow(records, schema, allocator)
    try {
      val vec = root.getVector("active").asInstanceOf[org.apache.arrow.vector.BitVector]
      assertEquals(vec.get(0), 1)
      assertEquals(vec.get(1), 0)
    } finally root.close()
  }

  test("toArrow handles null values") {
    val schema = SchemaMapper.toArrowSchema(
      SourceSchema.ObjectType(Map("name" -> SourceSchema.StringType()))
    )
    val records = List(parse("""{"name": null}""").toOption.get)

    val root = Converter.toArrow(records, schema, allocator)
    try {
      val vec = root.getVector("name").asInstanceOf[org.apache.arrow.vector.VarCharVector]
      assert(vec.isNull(0))
    } finally root.close()
  }

  test("toArrow navigates nested fields via json path metadata") {
    val schema = SchemaMapper.toArrowSchema(
      SourceSchema.ObjectType(Map(
        "address" -> SourceSchema.ObjectType(Map("city" -> SourceSchema.StringType()))
      ))
    )
    val records = List(parse("""{"address": {"city": "NYC"}}""").toOption.get)

    val root = Converter.toArrow(records, schema, allocator)
    try {
      val vec = root.getVector("address_city").asInstanceOf[org.apache.arrow.vector.VarCharVector]
      assertEquals(new String(vec.get(0), StandardCharsets.UTF_8), "NYC")
    } finally root.close()
  }

  test("toArrow converts float fields") {
    val schema = SchemaMapper.toArrowSchema(
      SourceSchema.ObjectType(Map("score" -> SourceSchema.NumberType()))
    )
    val records = List(parse("""{"score": 3.14}""").toOption.get)

    val root = Converter.toArrow(records, schema, allocator)
    try {
      val vec = root.getVector("score").asInstanceOf[org.apache.arrow.vector.Float8Vector]
      assertEquals(vec.get(0), 3.14, 0.001)
    } finally root.close()
  }

  test("toArrow serializes arrays as JSON strings") {
    val schema = SchemaMapper.toArrowSchema(
      SourceSchema.ObjectType(Map("tags" -> SourceSchema.ArrayType(SourceSchema.StringType())))
    )
    val records = List(parse("""{"tags": ["a", "b"]}""").toOption.get)

    val root = Converter.toArrow(records, schema, allocator)
    try {
      val vec = root.getVector("tags").asInstanceOf[org.apache.arrow.vector.VarCharVector]
      val value = new String(vec.get(0), StandardCharsets.UTF_8)
      assertEquals(value, """["a","b"]""")
    } finally root.close()
  }

  test("toArrow serializes VARIANT fields as JSON strings") {
    val schema = SchemaMapper.toArrowSchema(
      SourceSchema.ObjectType(Map(
        "id" -> SourceSchema.IntegerType(),
        "metadata" -> SourceSchema.VariantType
      ))
    )
    val records = List(
      parse("""{"id": 1, "metadata": {"key": "value", "nested": [1, 2]}}""").toOption.get,
      parse("""{"id": 2, "metadata": "just a string"}""").toOption.get,
      parse("""{"id": 3, "metadata": 42}""").toOption.get,
      parse("""{"id": 4, "metadata": null}""").toOption.get
    )

    val root = Converter.toArrow(records, schema, allocator)
    try {
      assertEquals(root.getRowCount, 4)
      val vec = root.getVector("metadata").asInstanceOf[org.apache.arrow.vector.VarCharVector]

      // Object serialized as compact JSON
      val obj = new String(vec.get(0), StandardCharsets.UTF_8)
      assertEquals(obj, """{"key":"value","nested":[1,2]}""")

      // String value preserved as-is
      val str = new String(vec.get(1), StandardCharsets.UTF_8)
      assertEquals(str, "just a string")

      // Number serialized as JSON
      val num = new String(vec.get(2), StandardCharsets.UTF_8)
      assertEquals(num, "42")

      // Null handled
      assert(vec.isNull(3))
    } finally root.close()
  }

  // ==========================================================================
  // Variant mode tests (#137)
  // ==========================================================================

  test("toArrow with variant schema stores entire record as JSON") {
    val schema = SchemaMapper.variantSchema()
    val records = List(
      parse("""{"id": 1, "name": "Alice", "nested": {"x": 10}}""").toOption.get,
      parse("""{"id": 2, "name": "Bob", "tags": ["a", "b"]}""").toOption.get
    )

    val root = Converter.toArrow(records, schema, allocator)
    try {
      assertEquals(root.getRowCount, 2)
      val vec = root.getVector(SchemaMapper.VariantColumnName).asInstanceOf[org.apache.arrow.vector.VarCharVector]

      // First record serialized as compact JSON
      val row1 = new String(vec.get(0), StandardCharsets.UTF_8)
      assert(row1.contains(""""id":1"""))
      assert(row1.contains(""""name":"Alice""""))
      assert(row1.contains(""""nested":{"x":10}"""))

      // Second record serialized as compact JSON
      val row2 = new String(vec.get(1), StandardCharsets.UTF_8)
      assert(row2.contains(""""id":2"""))
      assert(row2.contains(""""tags":["a","b"]"""))
    } finally root.close()
  }

  test("toArrow with variant schema handles null JSON") {
    val schema = SchemaMapper.variantSchema()
    val records = List(Json.Null)

    val root = Converter.toArrow(records, schema, allocator)
    try {
      val vec = root.getVector(SchemaMapper.VariantColumnName).asInstanceOf[org.apache.arrow.vector.VarCharVector]
      assert(vec.isNull(0))
    } finally root.close()
  }

  test("toArrow with variant schema preserves array records") {
    val schema = SchemaMapper.variantSchema()
    val records = List(
      parse("""[1, 2, 3]""").toOption.get,
      parse("""{"arr": [1, 2]}""").toOption.get
    )

    val root = Converter.toArrow(records, schema, allocator)
    try {
      val vec = root.getVector(SchemaMapper.VariantColumnName).asInstanceOf[org.apache.arrow.vector.VarCharVector]

      val row1 = new String(vec.get(0), StandardCharsets.UTF_8)
      assertEquals(row1, "[1,2,3]")

      val row2 = new String(vec.get(1), StandardCharsets.UTF_8)
      assert(row2.contains(""""arr":[1,2]"""))
    } finally root.close()
  }
}

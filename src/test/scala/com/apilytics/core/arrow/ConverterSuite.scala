package com.apilytics.core.arrow

import com.apilytics.core.openapi.OpenAPISchema
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
      OpenAPISchema.ObjectType(Map("name" -> OpenAPISchema.StringType()), Set("name"))
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
      OpenAPISchema.ObjectType(Map("count" -> OpenAPISchema.IntegerType()))
    )
    val records = List(parse("""{"count": 42}""").toOption.get)

    val root = Converter.toArrow(records, schema, allocator)
    try {
      val vec = root.getVector("count").asInstanceOf[org.apache.arrow.vector.IntVector]
      assertEquals(vec.get(0), 42)
    } finally root.close()
  }

  test("toArrow converts boolean fields") {
    val schema = SchemaMapper.toArrowSchema(
      OpenAPISchema.ObjectType(Map("active" -> OpenAPISchema.BooleanType))
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
      OpenAPISchema.ObjectType(Map("name" -> OpenAPISchema.StringType()))
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
      OpenAPISchema.ObjectType(Map(
        "address" -> OpenAPISchema.ObjectType(Map("city" -> OpenAPISchema.StringType()))
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
      OpenAPISchema.ObjectType(Map("score" -> OpenAPISchema.NumberType()))
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
      OpenAPISchema.ObjectType(Map("tags" -> OpenAPISchema.ArrayType(OpenAPISchema.StringType())))
    )
    val records = List(parse("""{"tags": ["a", "b"]}""").toOption.get)

    val root = Converter.toArrow(records, schema, allocator)
    try {
      val vec = root.getVector("tags").asInstanceOf[org.apache.arrow.vector.VarCharVector]
      val value = new String(vec.get(0), StandardCharsets.UTF_8)
      assertEquals(value, """["a","b"]""")
    } finally root.close()
  }
}

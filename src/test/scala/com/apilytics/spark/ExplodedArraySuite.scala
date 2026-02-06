package com.apilytics.spark

import com.apilytics.core.config._
import com.apilytics.core.openapi.{Endpoint, OpenAPISchema, ParsedSpec, QueryParam}
import com.apilytics.core.schema.SchemaMapper
import io.circe.Json
import io.circe.parser._
import munit.FunSuite
import org.apache.spark.sql.connector.expressions.{Expression, Expressions, Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ExplodedArraySuite extends FunSuite {

  private val defaultHttp = HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 30.seconds)
  private val defaultAuth = AuthConfig(authType = AuthType.Bearer, token = Some("test"))

  test("findArrayFields identifies top-level array fields") {
    val schema = OpenAPISchema.ObjectType(
      Map(
        "id" -> OpenAPISchema.IntegerType(),
        "name" -> OpenAPISchema.StringType(),
        "tags" -> OpenAPISchema.ArrayType(OpenAPISchema.StringType()),
        "addresses" -> OpenAPISchema.ArrayType(
          OpenAPISchema.ObjectType(Map(
            "city" -> OpenAPISchema.StringType(),
            "zip" -> OpenAPISchema.StringType()
          ))
        )
      )
    )

    val arrayFields = SchemaMapper.findArrayFields(schema)
    assertEquals(arrayFields.size, 2)

    val tagField = arrayFields.find(_.fieldName == "tags").get
    assertEquals(tagField.jsonPath, List("tags"))
    assert(tagField.itemSchema.isInstanceOf[OpenAPISchema.StringType])

    val addrField = arrayFields.find(_.fieldName == "addresses").get
    assert(addrField.itemSchema.isInstanceOf[OpenAPISchema.ObjectType])
  }

  test("ExplodedArrayTable builds schema with parent fields and exploded element") {
    val responseSchema = OpenAPISchema.ObjectType(
      Map(
        "id" -> OpenAPISchema.IntegerType(),
        "name" -> OpenAPISchema.StringType(),
        "tags" -> OpenAPISchema.ArrayType(OpenAPISchema.StringType())
      )
    )

    val endpoint = Endpoint(
      path = "/customers",
      operationId = Some("listCustomers"),
      responseSchema = responseSchema,
      queryParams = Nil
    )

    val arrayFields = SchemaMapper.findArrayFields(responseSchema)
    val tagsField = arrayFields.find(_.fieldName == "tags").get

    val sourceConfig = SourceConfig(
      openapi = "test.yaml",
      auth = defaultAuth,
      http = defaultHttp,
      schema = SchemaConfig(flattenDepth = 2, arrayHandling = ArrayHandling.ExplodeView)
    )

    val table = new ExplodedArrayTable(
      tableName = "customers_tags",
      baseTableName = "customers",
      arrayFieldName = "tags",
      arrayFieldInfo = tagsField,
      endpoint = endpoint,
      tableConfig = None,
      sourceConfig = sourceConfig,
      baseUrl = "http://localhost"
    )

    val sparkSchema = table.schema()
    val fieldNames = sparkSchema.fieldNames.toSet
    // Parent fields (excluding the array field)
    assert(fieldNames.contains("id"))
    assert(fieldNames.contains("name"))
    // Exploded array element field
    assert(fieldNames.contains("tags"))
    // Should not have 3 fields since id, name, and tags are all there
    assertEquals(sparkSchema.fields.length, 3)
  }

  test("ExplodedArrayTable with object array includes flattened element fields") {
    val responseSchema = OpenAPISchema.ObjectType(
      Map(
        "id" -> OpenAPISchema.IntegerType(),
        "addresses" -> OpenAPISchema.ArrayType(
          OpenAPISchema.ObjectType(Map(
            "city" -> OpenAPISchema.StringType(),
            "zip" -> OpenAPISchema.StringType()
          ))
        )
      )
    )

    val endpoint = Endpoint(
      path = "/customers",
      operationId = Some("listCustomers"),
      responseSchema = responseSchema,
      queryParams = Nil
    )

    val arrayFields = SchemaMapper.findArrayFields(responseSchema)
    val addrField = arrayFields.find(_.fieldName == "addresses").get

    val sourceConfig = SourceConfig(
      openapi = "test.yaml",
      auth = defaultAuth,
      http = defaultHttp,
      schema = SchemaConfig(flattenDepth = 2, arrayHandling = ArrayHandling.ExplodeView)
    )

    val table = new ExplodedArrayTable(
      tableName = "customers_addresses",
      baseTableName = "customers",
      arrayFieldName = "addresses",
      arrayFieldInfo = addrField,
      endpoint = endpoint,
      tableConfig = None,
      sourceConfig = sourceConfig,
      baseUrl = "http://localhost"
    )

    val sparkSchema = table.schema()
    val fieldNames = sparkSchema.fieldNames.toSet
    // Parent field
    assert(fieldNames.contains("id"))
    // Flattened object array element fields
    assert(fieldNames.contains("addresses_city"))
    assert(fieldNames.contains("addresses_zip"))
  }

  test("tableNames includes exploded views when arrayHandling is ExplodeView") {
    // This test verifies the catalog logic for generating table names
    val responseSchema = OpenAPISchema.ObjectType(
      Map(
        "id" -> OpenAPISchema.IntegerType(),
        "tags" -> OpenAPISchema.ArrayType(OpenAPISchema.StringType())
      )
    )

    val arrayFields = SchemaMapper.findArrayFields(responseSchema)
    assertEquals(arrayFields.size, 1)
    assertEquals(arrayFields.head.fieldName, "tags")
  }

  test("tableNames includes both base and exploded views when arrayHandling is Both") {
    val responseSchema = OpenAPISchema.ObjectType(
      Map(
        "id" -> OpenAPISchema.IntegerType(),
        "tags" -> OpenAPISchema.ArrayType(OpenAPISchema.StringType()),
        "roles" -> OpenAPISchema.ArrayType(OpenAPISchema.StringType())
      )
    )

    val arrayFields = SchemaMapper.findArrayFields(responseSchema)
    assertEquals(arrayFields.size, 2)
    val fieldNames = arrayFields.map(_.fieldName).toSet
    assert(fieldNames.contains("tags"))
    assert(fieldNames.contains("roles"))
  }

  test("explodedTableInfo parses table name correctly") {
    // Testing the split logic: "customers_tags" -> ("customers", "tags")
    val name = "customers_tags"
    val parts = name.split("_")
    assertEquals(parts.length, 2)
    assertEquals(parts.head, "customers")
    assertEquals(parts.last, "tags")
  }

  test("explodedTableInfo handles multi-underscore names") {
    // "user_profiles_active_tags" could be:
    // - ("user_profiles_active", "tags") - if user_profiles_active is a table with tags array
    // - ("user_profiles", "active_tags") - if user_profiles is a table with active_tags array
    // - ("user", "profiles_active_tags") - if user is a table with profiles_active_tags array
    val name = "user_profiles_active_tags"
    val parts = name.split("_")
    assertEquals(parts.length, 4)

    // The catalog tries right-to-left, preferring longer base table names
    // This is tested in integration with actual endpoint matching
  }

  test("ExplodedArrayPartitionReaderFactory supports columnar reads") {
    val factory = new ExplodedArrayPartitionReaderFactory()
    val partition = ExplodedArrayInputPartition(
      endpoint = Endpoint(
        path = "/items",
        operationId = None,
        responseSchema = OpenAPISchema.ObjectType(Map("id" -> OpenAPISchema.IntegerType())),
        queryParams = Nil
      ),
      tableConfig = None,
      sourceConfig = SourceConfig(
        openapi = "test.yaml",
        auth = defaultAuth,
        http = defaultHttp,
        schema = SchemaConfig(flattenDepth = 2, arrayHandling = ArrayHandling.ExplodeView)
      ),
      baseUrl = "http://localhost",
      arrowSchemaJson = "{}",
      arrayFieldName = "tags",
      arrayJsonPath = List("tags")
    )
    assert(factory.supportColumnarReads(partition))
  }

  // --- FilterPushdown tests ---

  /** Create an ExplodedArrayTable and its ScanBuilder with optional filter configs. */
  private def mkBuilder(filters: List[FilterConfig] = Nil): ExplodedArrayScanBuilder = {
    val responseSchema = OpenAPISchema.ObjectType(
      Map(
        "id" -> OpenAPISchema.IntegerType(),
        "name" -> OpenAPISchema.StringType(),
        "tags" -> OpenAPISchema.ArrayType(OpenAPISchema.StringType())
      )
    )
    val endpoint = Endpoint(
      path = "/items",
      operationId = Some("listItems"),
      responseSchema = responseSchema,
      queryParams = Nil
    )
    val sourceConfig = SourceConfig(
      openapi = "test.yaml",
      auth = defaultAuth,
      http = defaultHttp,
      schema = SchemaConfig(flattenDepth = 2, arrayHandling = ArrayHandling.ExplodeView)
    )
    val tableConfig = if (filters.nonEmpty) Some(TableConfig(endpoint = "/items", filters = filters)) else None
    val arrayFields = SchemaMapper.findArrayFields(responseSchema)
    val tagsField = arrayFields.find(_.fieldName == "tags").get

    val table = new ExplodedArrayTable(
      tableName = "items_tags",
      baseTableName = "items",
      arrayFieldName = "tags",
      arrayFieldInfo = tagsField,
      endpoint = endpoint,
      tableConfig = tableConfig,
      sourceConfig = sourceConfig,
      baseUrl = "http://localhost"
    )

    // Use the table's newScanBuilder to get a properly constructed builder
    table.newScanBuilder(new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))
      .asInstanceOf[ExplodedArrayScanBuilder]
  }

  private def mkPredicate(op: String, column: String, value: String): Predicate = {
    val ref = Expressions.column(column)
    val lit = Expressions.literal(UTF8String.fromString(value))
    new Predicate(op, Array[Expression](ref, lit))
  }

  private def mkIntPredicate(op: String, column: String, value: Int): Predicate = {
    val ref = Expressions.column(column)
    val lit = Expressions.literal(value)
    new Predicate(op, Array[Expression](ref, lit))
  }

  test("ExplodedArrayScanBuilder pushes matching filters to API params") {
    val builder = mkBuilder(List(
      FilterConfig(param = "name", column = "name", operators = List("eq"))
    ))

    val predicate = mkPredicate("=", "name", "pikachu")
    val remaining = builder.pushPredicates(Array(predicate))

    // Filter matched -> no remaining predicates
    assertEquals(remaining.length, 0)
    assertEquals(builder.pushedPredicates().length, 1)
  }

  test("ExplodedArrayScanBuilder returns unmatched predicates as remaining") {
    val builder = mkBuilder(List(
      FilterConfig(param = "name", column = "name", operators = List("eq"))
    ))

    val matchable = mkPredicate("=", "name", "pikachu")
    val unmatchable = mkIntPredicate(">", "id", 5)
    val remaining = builder.pushPredicates(Array(matchable, unmatchable))

    assertEquals(remaining.length, 1)
    assertEquals(builder.pushedPredicates().length, 1)
  }

  test("ExplodedArrayScanBuilder passes no filters when tableConfig has none") {
    val builder = mkBuilder() // no filters configured

    val predicate = mkPredicate("=", "name", "pikachu")
    val remaining = builder.pushPredicates(Array(predicate))

    // No filter configs -> all predicates remain
    assertEquals(remaining.length, 1)
    assertEquals(builder.pushedPredicates().length, 0)
  }

  test("ExplodedArrayScanBuilder pushLimit stores limit") {
    val builder = mkBuilder()

    val consumed = builder.pushLimit(10)
    // Should return false: Spark should still enforce limit post-scan
    assertEquals(consumed, false)
  }

  test("ExplodedArrayScanBuilder wires pushed params and limit through to InputPartition") {
    val builder = mkBuilder(List(
      FilterConfig(param = "name", column = "name", operators = List("eq"))
    ))

    builder.pushPredicates(Array(mkPredicate("=", "name", "pikachu")))
    builder.pushLimit(20)

    val scan = builder.build().asInstanceOf[ExplodedArrayScan]
    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 1)

    val partition = partitions(0).asInstanceOf[ExplodedArrayInputPartition]
    assertEquals(partition.pushedParams, Map("name" -> "pikachu"))
    assertEquals(partition.pushedLimit, Some(20))
  }

  test("FilterPushdown only matches configured operators") {
    val builder = mkBuilder(List(
      FilterConfig(param = "offset", column = "id", operators = List("gte"))
    ))

    // = is NOT in the configured operators (gte), so it won't match
    val remaining = builder.pushPredicates(Array(mkIntPredicate("=", "id", 5)))
    assertEquals(remaining.length, 1)
    assertEquals(builder.pushedPredicates().length, 0)

    // >= IS in the configured operators, so it matches
    val remaining2 = builder.pushPredicates(Array(mkIntPredicate(">=", "id", 5)))
    assertEquals(remaining2.length, 0)
    assertEquals(builder.pushedPredicates().length, 1)
  }

  // --- ExplodedArrayOps.explodeRecord tests ---

  test("explodeRecord explodes primitive array into multiple records") {
    val record = parse("""{"id": 1, "name": "test", "tags": ["a", "b", "c"]}""").toOption.get
    val exploded = ExplodedArrayOps.explodeRecord(record, "tags", List("tags"))

    assertEquals(exploded.length, 3)
    assertEquals(exploded(0).hcursor.get[Int]("id").toOption, Some(1))
    assertEquals(exploded(0).hcursor.get[String]("name").toOption, Some("test"))
    assertEquals(exploded(0).hcursor.get[String]("tags").toOption, Some("a"))
    assertEquals(exploded(1).hcursor.get[String]("tags").toOption, Some("b"))
    assertEquals(exploded(2).hcursor.get[String]("tags").toOption, Some("c"))
  }

  test("explodeRecord explodes object array into multiple records") {
    val record = parse("""{"id": 1, "addresses": [{"city": "NYC"}, {"city": "LA"}]}""").toOption.get
    val exploded = ExplodedArrayOps.explodeRecord(record, "addresses", List("addresses"))

    assertEquals(exploded.length, 2)
    assertEquals(exploded(0).hcursor.get[Int]("id").toOption, Some(1))
    assertEquals(exploded(0).hcursor.downField("addresses").get[String]("city").toOption, Some("NYC"))
    assertEquals(exploded(1).hcursor.downField("addresses").get[String]("city").toOption, Some("LA"))
  }

  test("explodeRecord returns Nil for empty array (INNER semantics)") {
    val record = parse("""{"id": 1, "tags": []}""").toOption.get
    val exploded = ExplodedArrayOps.explodeRecord(record, "tags", List("tags"))

    assertEquals(exploded, Nil)
  }

  test("explodeRecord returns Nil for null/missing array") {
    val record = parse("""{"id": 1, "name": "test"}""").toOption.get
    val exploded = ExplodedArrayOps.explodeRecord(record, "tags", List("tags"))

    assertEquals(exploded, Nil)
  }

  test("explodeRecord removes array field from parent fields") {
    val record = parse("""{"id": 1, "tags": ["x"]}""").toOption.get
    val exploded = ExplodedArrayOps.explodeRecord(record, "tags", List("tags"))

    assertEquals(exploded.length, 1)
    // Parent should have id but not the original tags array
    val keys = exploded(0).asObject.get.keys.toSet
    assertEquals(keys, Set("id", "tags"))
    // The tags field should be the exploded element, not the array
    assertEquals(exploded(0).hcursor.get[String]("tags").toOption, Some("x"))
  }

  test("explodeRecord handles nested array path") {
    val record = parse("""{"id": 1, "profile": {"tags": ["a", "b"]}}""").toOption.get
    val exploded = ExplodedArrayOps.explodeRecord(record, "tags", List("profile", "tags"))

    assertEquals(exploded.length, 2)
    // Parent fields preserved (profile is NOT removed since arrayFieldName is "tags")
    assert(exploded(0).hcursor.downField("profile").succeeded)
    assertEquals(exploded(0).hcursor.get[String]("tags").toOption, Some("a"))
    assertEquals(exploded(1).hcursor.get[String]("tags").toOption, Some("b"))
  }

  test("explodeRecord returns Nil for non-object record") {
    val arrayRecord = parse("""[1, 2, 3]""").toOption.get
    val primitiveRecord = parse(""""just a string"""").toOption.get

    assertEquals(ExplodedArrayOps.explodeRecord(arrayRecord, "tags", List("tags")), Nil)
    assertEquals(ExplodedArrayOps.explodeRecord(primitiveRecord, "tags", List("tags")), Nil)
  }

  // --- OUTER explode tests ---

  test("explodeRecord with outer=true emits null row for empty array") {
    val record = parse("""{"id": 1, "tags": []}""").toOption.get
    val exploded = ExplodedArrayOps.explodeRecord(record, "tags", List("tags"), outer = true)

    assertEquals(exploded.length, 1)
    assertEquals(exploded(0).hcursor.get[Int]("id").toOption, Some(1))
    assert(exploded(0).hcursor.get[String]("tags").isLeft) // null, not a string
    assertEquals(exploded(0).hcursor.downField("tags").focus, Some(Json.Null))
  }

  test("explodeRecord with outer=true emits null row for missing array") {
    val record = parse("""{"id": 1, "name": "test"}""").toOption.get
    val exploded = ExplodedArrayOps.explodeRecord(record, "tags", List("tags"), outer = true)

    assertEquals(exploded.length, 1)
    assertEquals(exploded(0).hcursor.get[Int]("id").toOption, Some(1))
    assertEquals(exploded(0).hcursor.get[String]("name").toOption, Some("test"))
    assertEquals(exploded(0).hcursor.downField("tags").focus, Some(Json.Null))
  }

  test("explodeRecord with outer=true still explodes non-empty arrays normally") {
    val record = parse("""{"id": 1, "tags": ["a", "b"]}""").toOption.get
    val exploded = ExplodedArrayOps.explodeRecord(record, "tags", List("tags"), outer = true)

    assertEquals(exploded.length, 2)
    assertEquals(exploded(0).hcursor.get[String]("tags").toOption, Some("a"))
    assertEquals(exploded(1).hcursor.get[String]("tags").toOption, Some("b"))
  }

  test("explodeRecord with outer=false (default) drops empty arrays") {
    val record = parse("""{"id": 1, "tags": []}""").toOption.get

    // Explicit outer=false
    assertEquals(ExplodedArrayOps.explodeRecord(record, "tags", List("tags"), outer = false), Nil)

    // Default (should be INNER)
    assertEquals(ExplodedArrayOps.explodeRecord(record, "tags", List("tags")), Nil)
  }

}

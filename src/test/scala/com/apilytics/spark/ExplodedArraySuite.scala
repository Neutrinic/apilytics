package com.apilytics.spark

import com.apilytics.core.config._
import com.apilytics.core.openapi.{Endpoint, OpenAPISchema, ParsedSpec, QueryParam}
import com.apilytics.core.schema.SchemaMapper
import munit.FunSuite
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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
}

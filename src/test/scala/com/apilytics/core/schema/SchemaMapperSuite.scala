package com.apilytics.core.schema

import com.apilytics.core.openapi.OpenAPISchema
import munit.FunSuite
import org.apache.arrow.vector.types.pojo.ArrowType

import scala.jdk.CollectionConverters._

class SchemaMapperSuite extends FunSuite {

  test("flat object maps to correct Arrow types") {
    val schema = OpenAPISchema.ObjectType(
      Map(
        "name" -> OpenAPISchema.StringType(),
        "age" -> OpenAPISchema.IntegerType(),
        "score" -> OpenAPISchema.NumberType(),
        "active" -> OpenAPISchema.BooleanType
      ),
      required = Set("name")
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val fields = arrow.getFields.asScala.toList
    assertEquals(fields.size, 4)

    val nameField = fields.find(_.getName == "name").get
    assert(nameField.getType.isInstanceOf[ArrowType.Utf8])
    assert(!nameField.isNullable)

    val ageField = fields.find(_.getName == "age").get
    assert(ageField.getType.isInstanceOf[ArrowType.Int])
    assertEquals(ageField.getType.asInstanceOf[ArrowType.Int].getBitWidth, 32)
    assert(ageField.isNullable)

    val scoreField = fields.find(_.getName == "score").get
    assert(scoreField.getType.isInstanceOf[ArrowType.FloatingPoint])

    val activeField = fields.find(_.getName == "active").get
    assert(activeField.getType.isInstanceOf[ArrowType.Bool])
  }

  test("nested object flattens with underscore naming") {
    val schema = OpenAPISchema.ObjectType(
      Map(
        "id" -> OpenAPISchema.IntegerType(),
        "address" -> OpenAPISchema.ObjectType(
          Map(
            "city" -> OpenAPISchema.StringType(),
            "zip" -> OpenAPISchema.StringType()
          )
        )
      )
    )

    val arrow = SchemaMapper.toArrowSchema(schema, maxDepth = 2)
    val names = arrow.getFields.asScala.map(_.getName).toSet
    assert(names.contains("address_city"))
    assert(names.contains("address_zip"))
    assert(names.contains("id"))
  }

  test("json path metadata is set correctly for nested fields") {
    val schema = OpenAPISchema.ObjectType(
      Map(
        "address" -> OpenAPISchema.ObjectType(
          Map("city" -> OpenAPISchema.StringType())
        )
      )
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val cityField = arrow.getFields.asScala.find(_.getName == "address_city").get
    val jsonPath = cityField.getMetadata.get(SchemaMapper.JsonPathKey)
    assertEquals(jsonPath, "address,city")
  }

  test("beyond maxDepth objects become VARCHAR") {
    val schema = OpenAPISchema.ObjectType(
      Map(
        "deep" -> OpenAPISchema.ObjectType(
          Map(
            "nested" -> OpenAPISchema.ObjectType(
              Map("value" -> OpenAPISchema.StringType())
            )
          )
        )
      )
    )

    // maxDepth=1 means only one level of flattening
    val arrow = SchemaMapper.toArrowSchema(schema, maxDepth = 1)
    val fields = arrow.getFields.asScala.toList
    val nestedField = fields.find(_.getName == "deep_nested").get
    assert(nestedField.getType.isInstanceOf[ArrowType.Utf8])
  }

  test("array fields become VARCHAR") {
    val schema = OpenAPISchema.ObjectType(
      Map("tags" -> OpenAPISchema.ArrayType(OpenAPISchema.StringType()))
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val field = arrow.getFields.asScala.head
    assertEquals(field.getName, "tags")
    assert(field.getType.isInstanceOf[ArrowType.Utf8])
  }

  test("date and datetime formats map correctly") {
    val schema = OpenAPISchema.ObjectType(
      Map(
        "birthday" -> OpenAPISchema.StringType(Some("date")),
        "created_at" -> OpenAPISchema.StringType(Some("date-time"))
      )
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val fields = arrow.getFields.asScala.toList

    val birthday = fields.find(_.getName == "birthday").get
    assert(birthday.getType.isInstanceOf[ArrowType.Date])

    val createdAt = fields.find(_.getName == "created_at").get
    assert(createdAt.getType.isInstanceOf[ArrowType.Timestamp])
  }

  test("flattening collision throws IllegalArgumentException") {
    val schema = OpenAPISchema.ObjectType(
      Map(
        "user_name" -> OpenAPISchema.StringType(),
        "user" -> OpenAPISchema.ObjectType(
          Map("name" -> OpenAPISchema.StringType())
        )
      )
    )

    intercept[IllegalArgumentException] {
      SchemaMapper.toArrowSchema(schema)
    }
  }

  test("int64 format maps to 64-bit int") {
    val schema = OpenAPISchema.ObjectType(
      Map("big_id" -> OpenAPISchema.IntegerType(Some("int64")))
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val field = arrow.getFields.asScala.head
    assertEquals(field.getType.asInstanceOf[ArrowType.Int].getBitWidth, 64)
  }

  test("VariantType maps to VARCHAR") {
    val schema = OpenAPISchema.ObjectType(
      Map(
        "id" -> OpenAPISchema.IntegerType(),
        "metadata" -> OpenAPISchema.VariantType
      )
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val field = arrow.getFields.asScala.find(_.getName == "metadata").get
    assert(field.getType.isInstanceOf[ArrowType.Utf8])
  }

  test("empty object maps to VARCHAR") {
    val schema = OpenAPISchema.ObjectType(
      Map(
        "id" -> OpenAPISchema.IntegerType(),
        "extra" -> OpenAPISchema.ObjectType(Map.empty)
      )
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val field = arrow.getFields.asScala.find(_.getName == "extra").get
    assert(field.getType.isInstanceOf[ArrowType.Utf8])
  }

}

package com.apilytics.core.schema

import com.apilytics.core.config.SchemaMode
import com.apilytics.core.schema.SourceSchema
import munit.FunSuite
import org.apache.arrow.vector.types.pojo.ArrowType

import scala.jdk.CollectionConverters._

class SchemaMapperSuite extends FunSuite {

  test("flat object maps to correct Arrow types") {
    val schema = SourceSchema.ObjectType(
      Map(
        "name" -> SourceSchema.StringType(),
        "age" -> SourceSchema.IntegerType(),
        "score" -> SourceSchema.NumberType(),
        "active" -> SourceSchema.BooleanType
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
    val schema = SourceSchema.ObjectType(
      Map(
        "id" -> SourceSchema.IntegerType(),
        "address" -> SourceSchema.ObjectType(
          Map(
            "city" -> SourceSchema.StringType(),
            "zip" -> SourceSchema.StringType()
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
    val schema = SourceSchema.ObjectType(
      Map(
        "address" -> SourceSchema.ObjectType(
          Map("city" -> SourceSchema.StringType())
        )
      )
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val cityField = arrow.getFields.asScala.find(_.getName == "address_city").get
    val jsonPath = cityField.getMetadata.get(SchemaMapper.JsonPathKey)
    assertEquals(jsonPath, "address,city")
  }

  test("beyond maxDepth objects become VARCHAR") {
    val schema = SourceSchema.ObjectType(
      Map(
        "deep" -> SourceSchema.ObjectType(
          Map(
            "nested" -> SourceSchema.ObjectType(
              Map("value" -> SourceSchema.StringType())
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
    val schema = SourceSchema.ObjectType(
      Map("tags" -> SourceSchema.ArrayType(SourceSchema.StringType()))
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val field = arrow.getFields.asScala.head
    assertEquals(field.getName, "tags")
    assert(field.getType.isInstanceOf[ArrowType.Utf8])
  }

  test("date and datetime formats map correctly") {
    val schema = SourceSchema.ObjectType(
      Map(
        "birthday" -> SourceSchema.StringType(Some("date")),
        "created_at" -> SourceSchema.StringType(Some("date-time"))
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
    val schema = SourceSchema.ObjectType(
      Map(
        "user_name" -> SourceSchema.StringType(),
        "user" -> SourceSchema.ObjectType(
          Map("name" -> SourceSchema.StringType())
        )
      )
    )

    intercept[IllegalArgumentException] {
      SchemaMapper.toArrowSchema(schema)
    }
  }

  test("int64 format maps to 64-bit int") {
    val schema = SourceSchema.ObjectType(
      Map("big_id" -> SourceSchema.IntegerType(Some("int64")))
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val field = arrow.getFields.asScala.head
    assertEquals(field.getType.asInstanceOf[ArrowType.Int].getBitWidth, 64)
  }

  test("VariantType maps to VARCHAR") {
    val schema = SourceSchema.ObjectType(
      Map(
        "id" -> SourceSchema.IntegerType(),
        "metadata" -> SourceSchema.VariantType
      )
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val field = arrow.getFields.asScala.find(_.getName == "metadata").get
    assert(field.getType.isInstanceOf[ArrowType.Utf8])
  }

  test("empty object maps to VARCHAR") {
    val schema = SourceSchema.ObjectType(
      Map(
        "id" -> SourceSchema.IntegerType(),
        "extra" -> SourceSchema.ObjectType(Map.empty)
      )
    )

    val arrow = SchemaMapper.toArrowSchema(schema)
    val field = arrow.getFields.asScala.find(_.getName == "extra").get
    assert(field.getType.isInstanceOf[ArrowType.Utf8])
  }

  // ==========================================================================
  // Schema Mode tests (#137)
  // ==========================================================================

  test("variant mode produces single 'value' column") {
    val schema = SourceSchema.ObjectType(
      Map(
        "id" -> SourceSchema.IntegerType(),
        "name" -> SourceSchema.StringType(),
        "nested" -> SourceSchema.ObjectType(Map("x" -> SourceSchema.IntegerType()))
      )
    )

    val arrow = SchemaMapper.toArrowSchemaWithMode(schema, maxDepth = 2, SchemaMode.Variant)
    val fields = arrow.getFields.asScala.toList

    assertEquals(fields.size, 1)
    assertEquals(fields.head.getName, SchemaMapper.VariantColumnName)
    assert(fields.head.getType.isInstanceOf[ArrowType.Utf8])
    assertEquals(fields.head.getMetadata.get(SchemaMapper.VariantKey), "true")
  }

  test("strict mode uses normal flattening") {
    val schema = SourceSchema.ObjectType(
      Map(
        "id" -> SourceSchema.IntegerType(),
        "name" -> SourceSchema.StringType()
      )
    )

    val arrow = SchemaMapper.toArrowSchemaWithMode(schema, maxDepth = 2, SchemaMode.Strict)
    val names = arrow.getFields.asScala.map(_.getName).toSet

    assertEquals(names, Set("id", "name"))
  }

  test("variantSchema creates correct schema structure") {
    val arrow = SchemaMapper.variantSchema()
    val fields = arrow.getFields.asScala.toList

    assertEquals(fields.size, 1)
    assertEquals(fields.head.getName, "value")
    assert(fields.head.isNullable)
    assert(fields.head.getType.isInstanceOf[ArrowType.Utf8])
    assertEquals(fields.head.getMetadata.get(SchemaMapper.VariantKey), "true")
  }

}

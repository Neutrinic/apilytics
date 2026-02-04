package com.apilytics.spark

import munit.FunSuite
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema => ArrowSchema}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.jdk.CollectionConverters._

class ColumnPruningSuite extends FunSuite {

  private def makeArrowSchema(fields: (String, ArrowType)*): ArrowSchema = {
    val arrowFields = fields.map { case (name, tpe) =>
      new Field(name, FieldType.nullable(tpe), null)
    }.toList.asJava
    new ArrowSchema(arrowFields)
  }

  test("pruneSchema filters Arrow schema to requested columns") {
    val fullSchema = makeArrowSchema(
      "id" -> new ArrowType.Int(32, true),
      "name" -> ArrowType.Utf8.INSTANCE,
      "email" -> ArrowType.Utf8.INSTANCE,
      "age" -> new ArrowType.Int(32, true)
    )

    val requiredSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    ))

    val prunedArrowSchema = ArrowSchemaConverter.pruneSchema(fullSchema, requiredSchema)

    assertEquals(prunedArrowSchema.getFields.size(), 2)
    assertEquals(prunedArrowSchema.getFields.get(0).getName, "id")
    assertEquals(prunedArrowSchema.getFields.get(1).getName, "name")
  }

  test("pruneSchema handles single column selection") {
    val fullSchema = makeArrowSchema(
      "id" -> new ArrowType.Int(32, true),
      "name" -> ArrowType.Utf8.INSTANCE,
      "description" -> ArrowType.Utf8.INSTANCE
    )

    val requiredSchema = StructType(Seq(
      StructField("name", StringType)
    ))

    val prunedArrowSchema = ArrowSchemaConverter.pruneSchema(fullSchema, requiredSchema)

    assertEquals(prunedArrowSchema.getFields.size(), 1)
    assertEquals(prunedArrowSchema.getFields.get(0).getName, "name")
  }

  test("pruneSchema preserves field metadata") {
    // Metadata like json.path is attached to Field objects
    val metadata = new java.util.HashMap[String, String]()
    metadata.put("json.path", "address,city")

    val arrowFields = List(
      new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
      new Field("address_city", new FieldType(true, ArrowType.Utf8.INSTANCE, null, metadata), null)
    ).asJava
    val fullSchema = new ArrowSchema(arrowFields)

    val requiredSchema = StructType(Seq(
      StructField("address_city", StringType)
    ))

    val prunedArrowSchema = ArrowSchemaConverter.pruneSchema(fullSchema, requiredSchema)

    assertEquals(prunedArrowSchema.getFields.size(), 1)
    val prunedField = prunedArrowSchema.getFields.get(0)
    assertEquals(prunedField.getName, "address_city")
    // Metadata should be preserved from original field
    assertEquals(prunedField.getMetadata.get("json.path"), "address,city")
  }

  test("Spark schema is returned from prunedSchema when set") {
    val fullSparkSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("email", StringType)
    ))

    val prunedSchema = Some(StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    )))

    val readSchema = prunedSchema.getOrElse(fullSparkSchema)

    assertEquals(readSchema.fieldNames.toList, List("id", "name"))
  }
}

package com.apilytics.core.schema

import com.apilytics.core.openapi.OpenAPISchema
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import scala.jdk.CollectionConverters._

object SchemaMapper {

  /** Convert an OpenAPI object schema to an Arrow Schema, flattening nested objects up to `maxDepth`. */
  def toArrowSchema(obj: OpenAPISchema.ObjectType, maxDepth: Int = 2): Schema = {
    val fields = flattenFields(obj.properties, obj.required, prefix = "", depth = 0, maxDepth = maxDepth)
    new Schema(fields.asJava)
  }

  private def flattenFields(
      properties: Map[String, OpenAPISchema],
      required: Set[String],
      prefix: String,
      depth: Int,
      maxDepth: Int
  ): List[Field] = {
    properties.toList.sortBy(_._1).flatMap { case (name, schema) =>
      val fullName = if (prefix.isEmpty) name else s"${prefix}_$name"
      val nullable = !required.contains(name)

      schema match {
        case OpenAPISchema.ObjectType(props, req) if depth < maxDepth =>
          flattenFields(props, req, fullName, depth + 1, maxDepth)

        case OpenAPISchema.ObjectType(_, _) =>
          // Depth exceeded → VARIANT (stored as string of JSON)
          List(field(fullName, new ArrowType.Utf8(), nullable))

        case OpenAPISchema.ArrayType(_) =>
          // Arrays → VARIANT (stored as string of JSON)
          List(field(fullName, new ArrowType.Utf8(), nullable))

        case OpenAPISchema.StringType(Some("date")) =>
          List(field(fullName, new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY), nullable))

        case OpenAPISchema.StringType(Some("date-time")) =>
          List(field(fullName, new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC"), nullable))

        case OpenAPISchema.StringType(_) =>
          List(field(fullName, new ArrowType.Utf8(), nullable))

        case OpenAPISchema.IntegerType(Some("int64")) =>
          List(field(fullName, new ArrowType.Int(64, true), nullable))

        case OpenAPISchema.IntegerType(_) =>
          List(field(fullName, new ArrowType.Int(32, true), nullable))

        case OpenAPISchema.NumberType(_) =>
          List(field(fullName, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), nullable))

        case OpenAPISchema.BooleanType =>
          List(field(fullName, new ArrowType.Bool(), nullable))

        case OpenAPISchema.UnknownType =>
          List(field(fullName, new ArrowType.Utf8(), nullable))
      }
    }
  }

  private def field(name: String, arrowType: ArrowType, nullable: Boolean): Field =
    new Field(name, new FieldType(nullable, arrowType, null), null)
}

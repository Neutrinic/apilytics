package com.apilytics.core.schema

import com.apilytics.core.openapi.OpenAPISchema
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import scala.jdk.CollectionConverters._

object SchemaMapper {

  /** Metadata key storing the original JSON path segments for navigating nested objects.
    * Stored as comma-separated keys: field "address_city" has json.path = "address,city".
    * The Converter uses this to navigate the original JSON without ambiguity from underscores. */
  val JsonPathKey = "json.path"

  /** Information about an array field for generating exploded views. */
  final case class ArrayFieldInfo(
      fieldName: String,
      jsonPath: List[String],
      itemSchema: OpenAPISchema
  )

  /** Find all top-level array fields in an object schema. */
  def findArrayFields(obj: OpenAPISchema.ObjectType): List[ArrayFieldInfo] = {
    obj.properties.toList.collect {
      case (name, OpenAPISchema.ArrayType(itemSchema)) =>
        ArrayFieldInfo(name, List(name), itemSchema)
    }
  }

  /** Convert an OpenAPI object schema to an Arrow Schema, flattening nested objects up to `maxDepth`. */
  def toArrowSchema(obj: OpenAPISchema.ObjectType, maxDepth: Int = 2): Schema = {
    val fields = flattenFields(obj.properties, obj.required, prefix = "", pathSegments = Nil, depth = 0, maxDepth = maxDepth)
    val duplicates = fields.groupBy(_.getName).collect { case (name, fs) if fs.size > 1 => name }
    if (duplicates.nonEmpty) {
      throw new IllegalArgumentException(
        s"Schema flattening produced duplicate field names: ${duplicates.mkString(", ")}. " +
          "This happens when a top-level field name collides with a flattened nested path (e.g. 'user_name' vs 'user.name')."
      )
    }
    new Schema(fields.asJava)
  }

  private def flattenFields(
      properties: Map[String, OpenAPISchema],
      required: Set[String],
      prefix: String,
      pathSegments: List[String],
      depth: Int,
      maxDepth: Int
  ): List[Field] = {
    properties.toList.sortBy(_._1).flatMap { case (name, schema) =>
      val fullName = if (prefix.isEmpty) name else s"${prefix}_$name"
      val segments = pathSegments :+ name
      val nullable = !required.contains(name)

      schema match {
        case OpenAPISchema.ObjectType(props, req) if depth < maxDepth =>
          flattenFields(props, req, fullName, segments, depth + 1, maxDepth)

        case OpenAPISchema.ObjectType(_, _) =>
          List(field(fullName, new ArrowType.Utf8(), nullable, segments))

        case OpenAPISchema.ArrayType(_) =>
          List(field(fullName, new ArrowType.Utf8(), nullable, segments))

        case OpenAPISchema.StringType(Some("date")) =>
          List(field(fullName, new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY), nullable, segments))

        case OpenAPISchema.StringType(Some("date-time")) =>
          List(field(fullName, new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC"), nullable, segments))

        case OpenAPISchema.StringType(_) =>
          List(field(fullName, new ArrowType.Utf8(), nullable, segments))

        case OpenAPISchema.IntegerType(Some("int64")) =>
          List(field(fullName, new ArrowType.Int(64, true), nullable, segments))

        case OpenAPISchema.IntegerType(_) =>
          List(field(fullName, new ArrowType.Int(32, true), nullable, segments))

        case OpenAPISchema.NumberType(_) =>
          List(field(fullName, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), nullable, segments))

        case OpenAPISchema.BooleanType =>
          List(field(fullName, new ArrowType.Bool(), nullable, segments))

        case OpenAPISchema.UnknownType =>
          List(field(fullName, new ArrowType.Utf8(), nullable, segments))
      }
    }
  }

  private def field(name: String, arrowType: ArrowType, nullable: Boolean, jsonPath: List[String]): Field = {
    val metadata = java.util.Map.of(JsonPathKey, jsonPath.mkString(","))
    new Field(name, new FieldType(nullable, arrowType, null, metadata), null)
  }
}

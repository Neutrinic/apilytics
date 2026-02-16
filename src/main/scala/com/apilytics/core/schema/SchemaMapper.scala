package com.apilytics.core.schema

import com.apilytics.core.config.SchemaMode
import com.apilytics.core.openapi.OpenAPISchema
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import scala.jdk.CollectionConverters._

object SchemaMapper {

  /** Metadata key storing the original JSON path segments for navigating nested objects.
    * Stored as comma-separated keys: field "address_city" has json.path = "address,city".
    * The Converter uses this to navigate the original JSON without ambiguity from underscores. */
  val JsonPathKey = "json.path"

  /** Metadata key indicating this field should receive the entire JSON response.
    * Used for variant mode where the whole response is returned as a single column. */
  val VariantKey = "variant"

  /** Default column name for variant mode output. */
  val VariantColumnName = "value"

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

  /** Create a schema based on the schema mode.
    *
    * - Strict: use OpenAPI schema with flattening
    * - Variant: single "value" column containing raw JSON
    * - Infer/Lenient: handled at runtime (returns OpenAPI schema as fallback)
    */
  def toArrowSchemaWithMode(obj: OpenAPISchema.ObjectType, maxDepth: Int, mode: SchemaMode): Schema = {
    mode match {
      case SchemaMode.Variant =>
        // Single column for entire JSON response
        variantSchema()
      case SchemaMode.Strict | SchemaMode.Lenient | SchemaMode.Infer =>
        // Use OpenAPI schema (infer mode will replace this at runtime)
        toArrowSchema(obj, maxDepth)
    }
  }

  /** Create a schema with a single VARIANT column for the entire response.
    * The column stores JSON as a string. Users can use parse_json() in Spark 4.0
    * to convert it to VARIANT type for efficient querying.
    */
  def variantSchema(): Schema = {
    val metadata = java.util.Map.of(VariantKey, "true")
    val field = new Field(VariantColumnName, new FieldType(true, new ArrowType.Utf8(), null, metadata), null)
    new Schema(java.util.List.of(field))
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
        case OpenAPISchema.ObjectType(props, req) if props.nonEmpty && depth < maxDepth =>
          flattenFields(props, req, fullName, segments, depth + 1, maxDepth)

        case OpenAPISchema.ObjectType(props, _) if props.nonEmpty =>
          // Beyond maxDepth - serialize as JSON string
          List(field(fullName, new ArrowType.Utf8(), nullable, segments))

        case OpenAPISchema.ObjectType(_, _) | OpenAPISchema.VariantType =>
          // Empty object or VARIANT (additionalProperties, anyOf, oneOf, missing type)
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

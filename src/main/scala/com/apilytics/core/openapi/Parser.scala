package com.apilytics.core.openapi

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.media.{Schema => SwaggerSchema}
import io.swagger.v3.parser.OpenAPIV3Parser
import io.swagger.v3.parser.converter.SwaggerConverter
import io.swagger.v3.parser.core.models.ParseOptions

import com.apilytics.core.schema.SourceSchema

import scala.jdk.CollectionConverters._

/** A discovered GET endpoint that returns an array of objects. */
@SerialVersionUID(1L)
final case class Endpoint(
    path: String,
    operationId: Option[String],
    responseSchema: SourceSchema.ObjectType,
    queryParams: List[QueryParam]
) extends Serializable

@SerialVersionUID(1L)
final case class QueryParam(
    name: String,
    schema: SourceSchema,
    required: Boolean
) extends Serializable

object Parser {
  import io.swagger.v3.parser.core.models.SwaggerParseResult

  def parse(specLocation: String): ParsedSpec = {
    val opts = parseOptions()
    parseWithFallback(
      () => new OpenAPIV3Parser().readLocation(specLocation, null, opts),
      () => new SwaggerConverter().readLocation(specLocation, null, opts)
    )
  }

  def parseContent(specJson: String): ParsedSpec = {
    val opts = parseOptions()
    parseWithFallback(
      () => new OpenAPIV3Parser().readContents(specJson, null, opts),
      () => new SwaggerConverter().readContents(specJson, null, opts)
    )
  }

  private def parseOptions(): ParseOptions = {
    val opts = new ParseOptions()
    opts.setResolve(true)
    opts.setResolveFully(true)
    opts
  }

  private def parseWithFallback(
      tryOpenApi3: () => SwaggerParseResult,
      trySwagger2: () => SwaggerParseResult
  ): ParsedSpec = {
    val result3x = tryOpenApi3()
    if (result3x.getOpenAPI != null) {
      return extractEndpoints(result3x.getOpenAPI)
    }

    val result2 = trySwagger2()
    if (result2.getOpenAPI != null) {
      return extractEndpoints(result2.getOpenAPI)
    }

    // Collect error messages from both parsers
    val msgs3x = Option(result3x.getMessages).map(_.asScala.toList).getOrElse(Nil)
    val msgs2 = Option(result2.getMessages).map(_.asScala.toList).getOrElse(Nil)
    val combined = (msgs3x, msgs2) match {
      case (Nil, Nil) => "unknown error"
      case (m3, Nil)  => s"OpenAPI 3.x: ${m3.mkString(", ")}"
      case (Nil, m2)  => s"Swagger 2.0: ${m2.mkString(", ")}"
      case (m3, m2)   => s"OpenAPI 3.x: ${m3.mkString(", ")}; Swagger 2.0: ${m2.mkString(", ")}"
    }
    throw new IllegalArgumentException(s"Failed to parse spec: $combined")
  }

  private def extractEndpoints(api: OpenAPI): ParsedSpec = {
    val baseUrl = Option(api.getServers)
      .flatMap(_.asScala.headOption)
      .map(_.getUrl)
      .getOrElse("")

    val endpoints = Option(api.getPaths).map(_.asScala).getOrElse(Map.empty).flatMap {
      case (path, pathItem) =>
        Option(pathItem.getGet).flatMap { op =>
          extractGetEndpoint(path, op)
        }
    }.toList

    ParsedSpec(baseUrl = baseUrl, endpoints = endpoints)
  }

  private def extractGetEndpoint(path: String, op: io.swagger.v3.oas.models.Operation): Option[Endpoint] = {
    val responseSchema = for {
      responses <- Option(op.getResponses)
      okResp    <- Option(responses.get("200")).orElse(Option(responses.get("default")))
      content   <- Option(okResp.getContent)
      json      <- Option(content.get("application/json"))
      schema    <- Option(json.getSchema)
    } yield schema

    responseSchema.flatMap { schema =>
      val parsed = convertSchema(schema)
      // We want endpoints that return objects (possibly wrapping arrays)
      parsed match {
        case obj: SourceSchema.ObjectType => Some(obj)
        case SourceSchema.ArrayType(obj: SourceSchema.ObjectType) =>
          // Top-level array response — wrap in a synthetic "data" key so the endpoint
          // has a consistent ObjectType schema. Callers should set data-path = "/data"
          // in table config to extract records from this wrapper.
          Some(SourceSchema.ObjectType(Map("data" -> SourceSchema.ArrayType(obj))))
        case _ => None
      }
    }.map { objSchema =>
      val params = Option(op.getParameters).map(_.asScala.toList).getOrElse(Nil)
        .filter(_.getIn == "query")
        .map { p =>
          QueryParam(
            name = p.getName,
            schema = Option(p.getSchema).map(convertSchema).getOrElse(SourceSchema.UnknownType),
            required = Option(p.getRequired).map(_.booleanValue()).getOrElse(false)
          )
        }

      Endpoint(
        path = path,
        operationId = Option(op.getOperationId),
        responseSchema = objSchema,
        queryParams = params
      )
    }
  }

  private def convertSchema(schema: SwaggerSchema[_]): SourceSchema = {
    if (schema == null) return SourceSchema.UnknownType

    // Union types (anyOf/oneOf) are ambiguous — map to VARIANT
    if (Option(schema.getAnyOf).exists(!_.isEmpty) || Option(schema.getOneOf).exists(!_.isEmpty)) {
      return SourceSchema.VariantType
    }

    // Check for additionalProperties: true (free-form object)
    val hasAdditionalProps = Option(schema.getAdditionalProperties).exists {
      case b: java.lang.Boolean => b
      case _: SwaggerSchema[_]  => true
      case _                    => false
    }

    // OpenAPI 3.0 uses getType(), OpenAPI 3.1 uses getTypes() (array of types)
    // For 3.1 with multiple non-null types (union), return VariantType
    val tpe = Option(schema.getType).map(_.toString)
      .orElse {
        // OpenAPI 3.1: getTypes() returns Set<String> like ["string", "null"]
        Option(schema.getTypes).flatMap { types =>
          val nonNullTypes = types.asScala.filterNot(_ == "null").toList
          nonNullTypes match {
            case Nil         => Some("null")
            case single :: Nil => Some(single)
            case _           => None // Multiple types = union, will fall through to VariantType
          }
        }
      }
      .orElse(Option(schema.get$ref).map(_ => "object"))
    val hasProps = schema.getProperties != null && !schema.getProperties.isEmpty

    tpe match {
      case Some("string")  => SourceSchema.StringType(Option(schema.getFormat))
      case Some("integer") => SourceSchema.IntegerType(Option(schema.getFormat))
      case Some("number")  => SourceSchema.NumberType(Option(schema.getFormat))
      case Some("boolean") => SourceSchema.BooleanType
      case Some("array") =>
        val items = Option(schema.getItems).map(convertSchema).getOrElse(SourceSchema.UnknownType)
        SourceSchema.ArrayType(items)
      case Some("object") if hasProps && !hasAdditionalProps =>
        // Object with defined properties - flatten to typed columns
        val props = schema.getProperties.asScala.map {
          case (name, propSchema) => name -> convertSchema(propSchema)
        }.toMap
        val required = Option(schema.getRequired).map(_.asScala.toSet).getOrElse(Set.empty)
        SourceSchema.ObjectType(props, required)
      case Some("object") =>
        // Object with additionalProperties or no properties - VARIANT
        SourceSchema.VariantType
      case None if hasProps =>
        // Missing type but has properties - treat as object
        val props = schema.getProperties.asScala.map {
          case (name, propSchema) => name -> convertSchema(propSchema)
        }.toMap
        val required = Option(schema.getRequired).map(_.asScala.toSet).getOrElse(Set.empty)
        SourceSchema.ObjectType(props, required)
      case None =>
        // Empty schema {} or missing type entirely - VARIANT
        SourceSchema.VariantType
      case _ => SourceSchema.UnknownType
    }
  }
}

@SerialVersionUID(1L)
final case class ParsedSpec(
    baseUrl: String,
    endpoints: List[Endpoint]
) extends Serializable

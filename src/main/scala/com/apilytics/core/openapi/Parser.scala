package com.apilytics.core.openapi

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.media.{Schema => SwaggerSchema}
import io.swagger.v3.parser.OpenAPIV3Parser
import io.swagger.v3.parser.core.models.ParseOptions

import scala.jdk.CollectionConverters._

/** Parsed representation of an OpenAPI schema property. */
sealed trait OpenAPISchema
object OpenAPISchema {
  final case class StringType(format: Option[String] = None) extends OpenAPISchema
  final case class IntegerType(format: Option[String] = None) extends OpenAPISchema
  final case class NumberType(format: Option[String] = None) extends OpenAPISchema
  case object BooleanType extends OpenAPISchema
  final case class ArrayType(items: OpenAPISchema) extends OpenAPISchema
  final case class ObjectType(properties: Map[String, OpenAPISchema], required: Set[String] = Set.empty) extends OpenAPISchema
  /** VARIANT for ambiguous schemas: additionalProperties, empty object, missing type, anyOf, oneOf */
  case object VariantType extends OpenAPISchema
  case object UnknownType extends OpenAPISchema
}

/** A discovered GET endpoint that returns an array of objects. */
final case class Endpoint(
    path: String,
    operationId: Option[String],
    responseSchema: OpenAPISchema.ObjectType,
    queryParams: List[QueryParam]
)

final case class QueryParam(
    name: String,
    schema: OpenAPISchema,
    required: Boolean
)

object Parser {

  def parse(specLocation: String): ParsedSpec = {
    val opts = new ParseOptions()
    opts.setResolve(true)
    opts.setResolveFully(true)

    val result = new OpenAPIV3Parser().readLocation(specLocation, null, opts)
    if (result.getOpenAPI == null) {
      val messages = Option(result.getMessages).map(_.asScala.mkString(", ")).getOrElse("unknown error")
      throw new IllegalArgumentException(s"Failed to parse OpenAPI spec: $messages")
    }
    extractEndpoints(result.getOpenAPI)
  }

  def parseContent(specJson: String): ParsedSpec = {
    val opts = new ParseOptions()
    opts.setResolve(true)
    opts.setResolveFully(true)

    val result = new OpenAPIV3Parser().readContents(specJson, null, opts)
    if (result.getOpenAPI == null) {
      val messages = Option(result.getMessages).map(_.asScala.mkString(", ")).getOrElse("unknown error")
      throw new IllegalArgumentException(s"Failed to parse OpenAPI spec: $messages")
    }
    extractEndpoints(result.getOpenAPI)
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
        case obj: OpenAPISchema.ObjectType => Some(obj)
        case OpenAPISchema.ArrayType(obj: OpenAPISchema.ObjectType) =>
          // Top-level array response — wrap in a synthetic "data" key so the endpoint
          // has a consistent ObjectType schema. Callers should set data-path = "/data"
          // in table config to extract records from this wrapper.
          Some(OpenAPISchema.ObjectType(Map("data" -> OpenAPISchema.ArrayType(obj))))
        case _ => None
      }
    }.map { objSchema =>
      val params = Option(op.getParameters).map(_.asScala.toList).getOrElse(Nil)
        .filter(_.getIn == "query")
        .map { p =>
          QueryParam(
            name = p.getName,
            schema = Option(p.getSchema).map(convertSchema).getOrElse(OpenAPISchema.UnknownType),
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

  private def convertSchema(schema: SwaggerSchema[_]): OpenAPISchema = {
    if (schema == null) return OpenAPISchema.UnknownType

    // Union types (anyOf/oneOf) are ambiguous — map to VARIANT
    if (Option(schema.getAnyOf).exists(!_.isEmpty) || Option(schema.getOneOf).exists(!_.isEmpty)) {
      return OpenAPISchema.VariantType
    }

    // Check for additionalProperties: true (free-form object)
    val hasAdditionalProps = Option(schema.getAdditionalProperties).exists {
      case b: java.lang.Boolean => b
      case _: SwaggerSchema[_]  => true
      case _                    => false
    }

    val tpe = Option(schema.getType).map(_.toString).orElse(Option(schema.get$ref).map(_ => "object"))
    val hasProps = schema.getProperties != null && !schema.getProperties.isEmpty

    tpe match {
      case Some("string")  => OpenAPISchema.StringType(Option(schema.getFormat))
      case Some("integer") => OpenAPISchema.IntegerType(Option(schema.getFormat))
      case Some("number")  => OpenAPISchema.NumberType(Option(schema.getFormat))
      case Some("boolean") => OpenAPISchema.BooleanType
      case Some("array") =>
        val items = Option(schema.getItems).map(convertSchema).getOrElse(OpenAPISchema.UnknownType)
        OpenAPISchema.ArrayType(items)
      case Some("object") if hasProps && !hasAdditionalProps =>
        // Object with defined properties - flatten to typed columns
        val props = schema.getProperties.asScala.map {
          case (name, propSchema) => name -> convertSchema(propSchema)
        }.toMap
        val required = Option(schema.getRequired).map(_.asScala.toSet).getOrElse(Set.empty)
        OpenAPISchema.ObjectType(props, required)
      case Some("object") =>
        // Object with additionalProperties or no properties - VARIANT
        OpenAPISchema.VariantType
      case None if hasProps =>
        // Missing type but has properties - treat as object
        val props = schema.getProperties.asScala.map {
          case (name, propSchema) => name -> convertSchema(propSchema)
        }.toMap
        val required = Option(schema.getRequired).map(_.asScala.toSet).getOrElse(Set.empty)
        OpenAPISchema.ObjectType(props, required)
      case None =>
        // Empty schema {} or missing type entirely - VARIANT
        OpenAPISchema.VariantType
      case _ => OpenAPISchema.UnknownType
    }
  }
}

final case class ParsedSpec(
    baseUrl: String,
    endpoints: List[Endpoint]
)

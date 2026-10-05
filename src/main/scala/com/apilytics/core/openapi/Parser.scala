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

    val gets = Option(api.getPaths).map(_.asScala).getOrElse(Map.empty).toList.flatMap {
      case (path, pathItem) => Option(pathItem.getGet).map(path -> _)
    }
    val endpoints = gets.flatMap { case (path, op) => extractGetEndpoint(path, op) }

    // GET endpoints with a response but none we read, and the content types they do offer,
    // so a table pointed at one can be told why it has no columns (#319).
    val read = endpoints.map(_.path).toSet
    val unreadable = gets.collect {
      case (path, op) if !read.contains(path) && okContent(op).exists(!_.isEmpty) =>
        path -> okContent(op).map(_.keySet.asScala.toList.sorted).getOrElse(Nil)
    }.toMap

    // Records an NDJSON or JSON Lines response describes, for sources that read NDJSON (#345).
    // Each line is one record, like the items of a top-level array, so the record is stored
    // the way a top-level array response is: wrapped as `data`, which table resolution unwraps.
    val ndjsonEndpoints = gets.flatMap { case (path, op) =>
      ndjsonRecordSchema(op).map { record =>
        Endpoint(
          path = path,
          operationId = Option(op.getOperationId),
          responseSchema = SourceSchema.ObjectType(Map("data" -> SourceSchema.ArrayType(record))),
          queryParams = queryParams(op)
        )
      }
    }

    // Every GET offering an NDJSON response, record schema or not, with all its content types.
    // An NDJSON source reads such a path through that response only (#345).
    val ndjsonAdvertised = gets.collect {
      case (path, op) if okContent(op).exists(_.keySet.asScala.exists(isNdjsonMediaType)) =>
        path -> okContent(op).map(_.keySet.asScala.toList.sorted).getOrElse(Nil)
    }.toMap

    ParsedSpec(
      baseUrl = baseUrl,
      endpoints = endpoints,
      unreadable = unreadable,
      ndjsonEndpoints = ndjsonEndpoints,
      ndjsonAdvertised = ndjsonAdvertised
    )
  }

  /** The record an NDJSON or JSON Lines response describes: its schema, if that's an object,
    * or the item of an array of objects, which some specs give for the stream as a whole.
    * None when there's no such response, or its schema is no record (`type: string`, say).
    */
  private def ndjsonRecordSchema(op: io.swagger.v3.oas.models.Operation): Option[SourceSchema.ObjectType] =
    okContent(op).flatMap { content =>
      content.asScala.toList
        .collect { case (key, media) if media != null && media.getSchema != null && isNdjsonMediaType(key) => media.getSchema }
        .iterator.map(convertSchema).collectFirst {
          case obj: SourceSchema.ObjectType                         => obj
          case SourceSchema.ArrayType(obj: SourceSchema.ObjectType) => obj
        }
    }

  // Media types for one JSON value per line, which is what the NDJSON reader parses.
  // `application/json-seq` isn't one: it starts each record with an RS character.
  private val ndjsonTypes = Set(
    "application/x-ndjson", "application/ndjson", "application/jsonl", "application/x-jsonlines",
    "application/jsonlines"
  )

  /** Whether a response media type is newline-delimited JSON, which an NDJSON source reads. */
  private[apilytics] def isNdjsonMediaType(key: String): Boolean = ndjsonTypes.contains(baseType(key))

  private def okContent(op: io.swagger.v3.oas.models.Operation): Option[io.swagger.v3.oas.models.media.Content] =
    for {
      responses <- Option(op.getResponses)
      okResp    <- Option(responses.get("200")).orElse(Option(responses.get("default")))
      content   <- Option(okResp.getContent)
    } yield content

  /** The JSON media type of a response, in order of preference (#319).
    *
    * Only an exact `application/json` used to count, so `application/json; charset=utf-8`,
    * `application/hal+json`, `application/vnd.api+json` and springdoc's default wildcard
    * type all dropped the endpoint, and a table configured on it resolved with no columns.
    * Now: `application/json`, then the same with parameters, then any `+json` type, then
    * the wildcard, among the media types that have a schema.
    */
  private[openapi] def jsonMediaType(content: io.swagger.v3.oas.models.media.Content): Option[io.swagger.v3.oas.models.media.MediaType] = {
    // A media type without a schema gives no columns, so it can't win over one that has.
    val entries = content.asScala.toList.filter(e => e._2 != null && e._2.getSchema != null)
    jsonPreferences.iterator.map(p => entries.find(e => p(e._1)).map(_._2)).collectFirst { case Some(m) => m }
  }

  private def baseType(key: String): String = key.split(';').head.trim.toLowerCase(java.util.Locale.ROOT)

  // RFC 6839 allows the `+json` suffix on any type, not only `application/`.
  private val jsonPreferences: List[String => Boolean] = List(
    _ == "application/json",
    baseType(_) == "application/json",
    baseType(_).endsWith("+json"),
    baseType(_) == "*/*"
  )

  /** Whether a response media type is one the reader takes as JSON. */
  private[apilytics] def isJsonMediaType(key: String): Boolean = jsonPreferences.exists(_(key))

  private def extractGetEndpoint(path: String, op: io.swagger.v3.oas.models.Operation): Option[Endpoint] = {
    val responseSchema = for {
      content <- okContent(op)
      json    <- jsonMediaType(content)
      schema  <- Option(json.getSchema)
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
      Endpoint(
        path = path,
        operationId = Option(op.getOperationId),
        responseSchema = objSchema,
        queryParams = queryParams(op)
      )
    }
  }

  private def queryParams(op: io.swagger.v3.oas.models.Operation): List[QueryParam] =
    Option(op.getParameters).map(_.asScala.toList).getOrElse(Nil)
      .filter(_.getIn == "query")
      .map { p =>
        QueryParam(
          name = p.getName,
          schema = Option(p.getSchema).map(convertSchema).getOrElse(SourceSchema.UnknownType),
          required = Option(p.getRequired).map(_.booleanValue()).getOrElse(false)
        )
      }

  private def convertSchema(schema: SwaggerSchema[_]): SourceSchema = {
    if (schema == null) return SourceSchema.UnknownType

    // Union types (anyOf/oneOf) are ambiguous — map to VARIANT
    if (Option(schema.getAnyOf).exists(!_.isEmpty) || Option(schema.getOneOf).exists(!_.isEmpty)) {
      return SourceSchema.VariantType
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
      case Some("object") if hasProps =>
        // Object with declared properties - flatten to typed columns. `additionalProperties`
        // doesn't change that: it only allows fields beyond those listed, which JSON Schema
        // allows by default anyway, and an undeclared field is skipped like any other. It used
        // to turn the whole object into a VARIANT, throwing the declared fields away, which
        // on a row object collapsed the table to its response wrapper (#341).
        val props = schema.getProperties.asScala.map {
          case (name, propSchema) => name -> convertSchema(propSchema)
        }.toMap
        val required = Option(schema.getRequired).map(_.asScala.toSet).getOrElse(Set.empty)
        SourceSchema.ObjectType(props, required)
      case Some("object") =>
        // No declared properties: a free-form object, or a map (`additionalProperties` alone,
        // such as `{"en": "...", "fr": "..."}`), whose keys aren't known up front - VARIANT
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
    endpoints: List[Endpoint],
    /** GET paths with a response in no format we read, and the content types they offer. */
    unreadable: Map[String, List[String]] = Map.empty,
    /** GET endpoints whose NDJSON or JSON Lines response describes a record, stored as a
      * top-level array response is (`data` wrapping the record). Used only by NDJSON sources.
      */
    ndjsonEndpoints: List[Endpoint] = Nil,
    /** GET paths offering an NDJSON or JSON Lines response, whether or not it describes a
      * record, with all the content types they offer.
      */
    ndjsonAdvertised: Map[String, List[String]] = Map.empty
) extends Serializable

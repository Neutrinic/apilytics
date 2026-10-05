package com.apilytics.core.rest

import com.apilytics.core.config.{ResponseFormat, SourceConfig}
import com.apilytics.core.openapi.{Endpoint, ParsedSpec, Parser, SpecCache}
import com.apilytics.core.schema.SourceSchema
import com.apilytics.core.source.{SourceCatalog, TableSpec}
import org.slf4j.LoggerFactory

/** REST implementation of the discovery interface (#191).
  *
  * Everything about turning an OpenAPI spec plus table config into readable tables lives
  * here: matching endpoints by path, template or operation id, synthesising endpoints for
  * config-only tables, and resolving a response schema down to the record schema. None of
  * that is visible to callers, who see only [[TableSpec]].
  */
final class RestSourceCatalog(config: SourceConfig) extends SourceCatalog {

  private val log = LoggerFactory.getLogger(getClass)

  private val spec: ParsedSpec = SpecCache.getOrParse(config.openapi, config.cache)

  /** Whether this source reads NDJSON, so a table can take its columns from the record an
    * NDJSON response describes. A JSON source can't read that body, so it never does (#345).
    */
  private val readsNdjson = config.http.responseFormat == ResponseFormat.NDJSON

  /** Endpoints a table can resolve to. For an NDJSON source, a path that offers an NDJSON
    * response is read through it, so its columns come from the record that response
    * describes, or the table has none. Its JSON response, if it also has one, describes a
    * different document: borrowing that schema put the lines' fields under the wrong columns,
    * silently NULL. Paths with no NDJSON response keep their JSON schema.
    */
  private def candidates: List[Endpoint] =
    if (readsNdjson) spec.ndjsonEndpoints ++ spec.endpoints.filterNot(e => spec.ndjsonAdvertised.contains(e.path))
    else spec.endpoints

  private def exactEndpoint(path: String): Option[Endpoint] = candidates.find(_.path == path)

  private def templateEndpoint(path: String): Option[Endpoint] =
    candidates.find(ep => pathMatches(path, ep.path))

  /** Base URL, preferring an explicit override over the spec's own server entry. */
  val baseUrl: String = config.baseUrl.getOrElse(spec.baseUrl)

  override def tableNames: Seq[String] = {
    val configured = config.tables.keys.toSeq
    // Only auto-discover endpoints without path parameters (e.g. /pokemon but not
    // /pokemon/{id}) — parameterised paths need parent-child config to supply values.
    val discovered = candidates
      .filterNot(_.path.contains("{"))
      .flatMap(ep => ep.operationId.orElse(ep.path.split("/").filterNot(_.isEmpty).lastOption))
    (configured ++ discovered).distinct
  }

  override def table(name: String): Option[TableSpec] =
    resolveEndpoint(name).map { endpoint =>
      TableSpec(
        name = name,
        schema = recordSchema(endpoint, name),
        handle = RestHandle(endpoint.path, baseUrl, config.tables.get(name))
      )
    }

  /** The endpoint backing a table, as the reader should call it.
    *
    * A table's configured endpoint wins over the spec's, because config carries concrete
    * path parameters where the spec has placeholders — "/repos/octocat/Hello-World/issues"
    * against "/repos/{owner}/{repo}/issues".
    */
  def resolveEndpoint(name: String): Option[Endpoint] =
    findEndpoint(name) match {
      case Some(specEndpoint) =>
        Some(config.tables.get(name).map(tc => specEndpoint.copy(path = tc.endpoint)).getOrElse(specEndpoint))
      case None =>
        // Config-only table: useful for streaming formats where the spec may not describe
        // the endpoint, or uses external $refs we cannot follow.
        config.tables.get(name).map { tc =>
          Endpoint(
            path = tc.endpoint,
            operationId = Some(name),
            responseSchema = SourceSchema.ObjectType(Map.empty),
            queryParams = Nil
          )
        }
    }

  /** Why a configured table has no endpoint in the spec to take its columns from, or None
    * when it has one (#319).
    *
    * Such a table falls back to a config-only endpoint with no columns, which suits variant
    * mode and streaming formats the spec doesn't describe. In strict mode it's a table with
    * no columns, never what was meant, so the caller fails with this reason instead. When the
    * spec does describe the path, but only in formats the reader doesn't read, it lists them.
    */
  def unmatchedReason(name: String): Option[String] =
    config.tables.get(name).flatMap { tc =>
      // A concrete path in the spec wins over a template, as OpenAPI matches them: a CSV
      // `/reports/latest` mustn't take its columns from a JSON `/reports/{id}`. For an NDJSON
      // source, so does a concrete path offering NDJSON, record schema or not.
      val unreadable = spec.unreadable ++ (if (readsNdjson) spec.ndjsonAdvertised else Map.empty)
      val exactUnreadable = unreadable.get(tc.endpoint)
      val byPath = exactEndpoint(tc.endpoint)
        .orElse(if (exactUnreadable.isDefined) None else templateEndpoint(tc.endpoint))
      val offered = exactUnreadable.orElse(unreadable.collectFirst {
        case (path, types) if pathMatches(tc.endpoint, path) => types
      })
      // A table whose own path the spec describes without usable JSON fails even if another
      // endpoint shares its name: borrowing that endpoint's schema gave the wrong columns.
      val unmatched = byPath.isEmpty && (offered.isDefined || findEndpoint(name).isEmpty)
      Option.when(unmatched) {
        val why = offered match {
          case Some(types) if readsNdjson && types.exists(Parser.isNdjsonMediaType) =>
            s"The spec describes it as ${types.mkString(", ")}, but with no record schema " +
              "under it to take columns from."
          case Some(types) if types.exists(Parser.isJsonMediaType) =>
            s"The spec describes it as ${types.mkString(", ")}, but with no object schema to " +
              "take columns from."
          case Some(types) =>
            s"The spec describes it, but its response is only ${types.mkString(", ")}, none of " +
              "them JSON."
          case None =>
            "The spec doesn't describe a GET endpoint at that path."
        }
        val fix =
          if (readsNdjson) "or describe the records under the endpoint's NDJSON response in the spec."
          else "or an endpoint the spec describes with a JSON response."
        s"Table '$name' has endpoint '${tc.endpoint}', which has no response schema to take " +
          s"columns from, so in strict mode it would have none. $why Use schema mode " +
          s"'variant', $fix"
      }
    }

  /** Record schema for a table: the response schema resolved down to one row. */
  def recordSchema(endpoint: Endpoint, name: String): SourceSchema.ObjectType =
    config.tables.get(name).flatMap(_.dataPath) match {
      case Some(dataPath) =>
        schemaAtPath(endpoint.responseSchema, dataPath).getOrElse {
          log.warn(
            "Could not extract schema at data-path '{}' for table '{}', using full response schema",
            dataPath, name
          )
          endpoint.responseSchema
        }
      case None =>
        unwrapSyntheticArrayWrapper(endpoint.responseSchema)
    }

  private def findEndpoint(name: String): Option[Endpoint] =
    config.tables
      .get(name)
      .flatMap { tc =>
        exactEndpoint(tc.endpoint).orElse(templateEndpoint(tc.endpoint))
      }
      .orElse {
        candidates.find { ep =>
          ep.operationId.contains(name) ||
          ep.path.split("/").filterNot(s => s.startsWith("{") || s.isEmpty).lastOption
            .exists(_.equalsIgnoreCase(name))
        }
      }

  /** Match a concrete config path against the spec's parameterised paths.
    *
    * A spec `{param}` segment matches any config segment, concrete or placeholder, so
    * "/repos/octocat/Hello-World/issues" matches "/repos/{owner}/{repo}/issues" and
    * "/customers/{customer_id}/orders" matches "/customers/{id}/orders".
    */
  def findByPathTemplate(configPath: String): Option[Endpoint] =
    spec.endpoints.find(ep => pathMatches(configPath, ep.path))

  private def pathMatches(configPath: String, specPath: String): Boolean = {
    val configSegments = configPath.split("/").toList
    val specSegments   = specPath.split("/").toList
    configSegments.length == specSegments.length &&
    configSegments.zip(specSegments).forall { case (configSeg, specSeg) =>
      specSeg.startsWith("{") || configSeg == specSeg
    }
  }

  /** Navigate a JSON-pointer path into a schema, returning the item schema for arrays. */
  private def schemaAtPath(schema: SourceSchema.ObjectType, path: String): Option[SourceSchema.ObjectType] = {
    def navigate(current: SourceSchema, remaining: List[String]): Option[SourceSchema.ObjectType] =
      remaining match {
        case Nil =>
          current match {
            case obj: SourceSchema.ObjectType => Some(obj)
            case arr: SourceSchema.ArrayType =>
              arr.items match {
                case obj: SourceSchema.ObjectType => Some(obj)
                case _                            => None
              }
            case _ => None
          }
        case segment :: rest =>
          current match {
            case obj: SourceSchema.ObjectType => obj.properties.get(segment).flatMap(navigate(_, rest))
            case arr: SourceSchema.ArrayType  => navigate(arr.items, segment :: rest)
            case _                            => None
          }
      }

    navigate(schema, path.stripPrefix("/").split("/").filter(_.nonEmpty).toList)
  }

  /** Unwrap the wrapper the parser synthesises for top-level array responses.
    *
    * An API returning `[{...}]` is parsed as `{data: [{...}]}`, so a lone `data` array
    * property means the real record schema is its item type.
    */
  private def unwrapSyntheticArrayWrapper(schema: SourceSchema.ObjectType): SourceSchema.ObjectType =
    schema.properties.get("data") match {
      case Some(SourceSchema.ArrayType(obj: SourceSchema.ObjectType)) if schema.properties.size == 1 => obj
      case _                                                                                        => schema
    }
}

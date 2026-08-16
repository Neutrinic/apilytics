package com.apilytics.core.rest

import com.apilytics.core.config.SourceConfig
import com.apilytics.core.openapi.{Endpoint, ParsedSpec, SpecCache}
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

  /** Base URL, preferring an explicit override over the spec's own server entry. */
  val baseUrl: String = config.baseUrl.getOrElse(spec.baseUrl)

  override def tableNames: Seq[String] = {
    val configured = config.tables.keys.toSeq
    // Only auto-discover endpoints without path parameters (e.g. /pokemon but not
    // /pokemon/{id}) — parameterised paths need parent-child config to supply values.
    val discovered = spec.endpoints
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
        spec.endpoints.find(_.path == tc.endpoint).orElse(findByPathTemplate(tc.endpoint))
      }
      .orElse {
        spec.endpoints.find { ep =>
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
  def findByPathTemplate(configPath: String): Option[Endpoint] = {
    val configSegments = configPath.split("/").toList
    spec.endpoints.find { ep =>
      val specSegments = ep.path.split("/").toList
      configSegments.length == specSegments.length &&
      configSegments.zip(specSegments).forall { case (configSeg, specSeg) =>
        specSeg.startsWith("{") || configSeg == specSeg
      }
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

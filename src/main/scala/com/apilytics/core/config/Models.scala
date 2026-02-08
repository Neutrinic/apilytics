package com.apilytics.core.config

import scala.concurrent.duration.FiniteDuration

sealed trait AuthType
object AuthType {
  case object None extends AuthType
  case object Bearer extends AuthType
  case object Basic extends AuthType
  case object Header extends AuthType
  case object OAuth2Client extends AuthType
}

sealed trait PaginationStyle
object PaginationStyle {
  case object Cursor extends PaginationStyle
  case object Offset extends PaginationStyle
  case object LinkHeader extends PaginationStyle
  case object None extends PaginationStyle
}

sealed trait ArrayHandling
object ArrayHandling {
  case object KeepArray extends ArrayHandling
  case object ExplodeView extends ArrayHandling
  case object Both extends ArrayHandling
}

final case class AuthConfig(
    authType: AuthType,
    token: Option[String] = None,
    username: Option[String] = None,
    password: Option[String] = None,
    headerName: Option[String] = None,
    headerValue: Option[String] = None,
    clientId: Option[String] = None,
    clientSecret: Option[String] = None,
    tokenUrl: Option[String] = None
)

final case class PaginationConfig(
    style: PaginationStyle = PaginationStyle.None,
    cursorPath: Option[String] = None,
    cursorParam: Option[String] = None,
    offsetParam: Option[String] = None,
    pageSizeParam: Option[String] = None,
    maxPageSize: Int = 100,
    /** JSON pointer to the results array in the response (e.g. "/results").
      * Used by offset pagination to detect empty pages and stop.
      * Without this, offset pagination falls back to the max-pages safety limit. */
    resultsPath: Option[String] = None,
    /** Maximum number of pages to fetch before stopping. Prevents infinite loops
      * when the API doesn't signal end-of-data. Default: 1000. */
    maxPages: Int = 1000
)

final case class SchemaConfig(
    flattenDepth: Int = 2,
    arrayHandling: ArrayHandling = ArrayHandling.KeepArray,
    arrowBatchSize: Int = 4096,
    /** When true, empty/null arrays emit one row with null element (OUTER semantics).
      * When false (default), empty arrays produce no rows (INNER semantics). */
    explodeOuter: Boolean = false
)

final case class HttpConfig(
    maxRetries: Int = 5,
    maxBackoff: FiniteDuration,
    timeout: FiniteDuration,
    /** Maximum requests per second. None means no rate limiting. */
    rateLimit: Option[Int] = None
)

final case class FilterConfig(
    param: String,
    column: String,
    operators: List[String]
)

sealed trait JoinStrategy
object JoinStrategy {
  /** For each parent row, fetch child endpoint with substituted path parameter. */
  case object NestedLoop extends JoinStrategy
}

final case class TableConfig(
    endpoint: String,
    dataPath: Option[String] = None,
    filters: List[FilterConfig] = Nil,
    /** Parent table name for parent-child joins (e.g., "customers"). */
    parentTable: Option[String] = None,
    /** Field from parent table to substitute into endpoint path (e.g., "id"). */
    parentKey: Option[String] = None,
    /** Strategy for joining parent and child data. */
    joinStrategy: Option[JoinStrategy] = None
)

final case class CacheConfig(
    /** Enable caching of parsed OpenAPI specs. */
    enabled: Boolean = false,
    /** Time-to-live for cached specs. None means indefinite (relies on ETag/mtime). */
    ttl: Option[FiniteDuration] = None,
    /** Cache directory. Defaults to ~/.apilytics/cache/ */
    directory: Option[String] = None
)

final case class SourceConfig(
    openapi: String,
    auth: AuthConfig,
    pagination: PaginationConfig = PaginationConfig(),
    schema: SchemaConfig = SchemaConfig(),
    http: HttpConfig,
    tables: Map[String, TableConfig] = Map.empty,
    /** Override the base URL from the OpenAPI spec. Useful when the spec doesn't
      * include servers or when you want to point to a different environment. */
    baseUrl: Option[String] = None,
    /** Cache configuration for parsed OpenAPI specs. */
    cache: CacheConfig = CacheConfig()
)

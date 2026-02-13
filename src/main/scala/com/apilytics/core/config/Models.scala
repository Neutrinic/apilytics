package com.apilytics.core.config

import scala.concurrent.duration.FiniteDuration

sealed trait AuthType extends Serializable
object AuthType {
  case object None extends AuthType
  case object Bearer extends AuthType
  case object Basic extends AuthType
  case object Header extends AuthType
  case object OAuth2Client extends AuthType
}

sealed trait PaginationStyle extends Serializable
object PaginationStyle {
  case object Cursor extends PaginationStyle
  case object Offset extends PaginationStyle
  case object LinkHeader extends PaginationStyle
  case object None extends PaginationStyle
}

sealed trait ArrayHandling extends Serializable
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

sealed trait ResponseCacheBackend extends Serializable
object ResponseCacheBackend {
  case object Memory extends ResponseCacheBackend
  // Future: Disk, Redis
}

final case class ResponseCacheConfig(
    /** Enable response caching. */
    enabled: Boolean = false,
    /** Cache backend. Currently only memory is supported. */
    backend: ResponseCacheBackend = ResponseCacheBackend.Memory,
    /** Time-to-live for cached responses. */
    ttl: FiniteDuration = FiniteDuration(5, "minutes"),
    /** Maximum number of cached entries (for memory backend). */
    maxEntries: Int = 1000
)

final case class HttpConfig(
    maxRetries: Int = 5,
    maxBackoff: FiniteDuration,
    timeout: FiniteDuration,
    /** Maximum requests per second (global limit). None means no rate limiting.
      *
      * When using date-range partitioning, the configured rate limit is automatically
      * distributed across partitions. For example, with `rate-limit = 100` and 10 partitions,
      * each partition will be limited to 10 rps to maintain the overall 100 rps limit.
      *
      * Edge case: If the rate limit is less than the number of partitions (e.g., 5 rps with
      * 10 partitions), each partition gets a minimum of 1 rps with a warning that the total
      * may exceed the configured limit. */
    rateLimit: Option[Int] = None,
    /** Response caching configuration. */
    responseCache: ResponseCacheConfig = ResponseCacheConfig()
)

final case class FilterConfig(
    param: String,
    column: String,
    operators: List[String]
)

/** Configuration for date-range partitioning to enable parallel reads.
  *
  * Partitions are executed in parallel by Spark executors. Each partition creates
  * its own HTTP client with independent rate limiting. See HttpConfig.rateLimit
  * for guidance on setting appropriate rate limits with partitioned reads.
  *
  * Note: `pushedLimit` (from LIMIT N) applies per-partition, not globally.
  * A query with LIMIT 10 and 5 partitions may fetch up to 50 rows before
  * Spark applies the final limit. */
final case class PartitionConfig(
    /** Column to partition by (must be a date/datetime column).
      * Currently used for documentation/validation; the actual partitioning
      * relies on startParam/endParam matching pushed filter params. */
    column: String,
    /** Size of each partition (e.g., "1 day", "1 hour", "7 days"). */
    range: FiniteDuration,
    /** API parameter for start of range (inclusive). */
    startParam: String,
    /** API parameter for end of range (exclusive). */
    endParam: String,
    /** Format for date parameters (default: ISO 8601). */
    format: String = "yyyy-MM-dd'T'HH:mm:ss'Z'"
)

sealed trait JoinStrategy extends Serializable
object JoinStrategy {
  /** For each parent row, fetch child endpoint with substituted path parameter. */
  case object NestedLoop extends JoinStrategy
  /** Fetch child records in batches by collecting parent keys. */
  case object Batch extends JoinStrategy
}

/** Configuration for COUNT(*) aggregation pushdown. */
final case class CountConfig(
    /** Endpoint that returns the count (e.g., "/items/count").
      * If not specified, uses the main endpoint with countParam. */
    endpoint: Option[String] = None,
    /** Query parameter to request count (e.g., "include" for ?include=total_count).
      * Only used if endpoint is not specified. */
    param: Option[String] = None,
    /** Value for the count query parameter (default: "true"). */
    paramValue: String = "true",
    /** JSON pointer to extract count from response (e.g., "/total_count"). */
    responsePath: String
)

final case class TableConfig(
    endpoint: String,
    dataPath: Option[String] = None,
    filters: List[FilterConfig] = Nil,
    /** Parent table name for parent-child joins (e.g., "customers"). */
    parentTable: Option[String] = None,
    /** Field from parent table to substitute into endpoint path (e.g., "id"). */
    parentKey: Option[String] = None,
    /** Strategy for joining parent and child data. */
    joinStrategy: Option[JoinStrategy] = None,
    /** Date-range partitioning configuration for parallel reads. */
    partition: Option[PartitionConfig] = None,
    /** Query parameter name for batch ID lookup (batch join strategy). */
    batchParam: Option[String] = None,
    /** Maximum number of IDs per batch request (batch join strategy). */
    batchSize: Int = 100,
    /** Separator for batch IDs in query param. Default: comma. */
    batchSeparator: String = ",",
    /** Field in child records that references the parent (batch join strategy).
      * Defaults to parentKey if not specified. */
    childKeyField: Option[String] = None,
    /** Configuration for COUNT(*) aggregation pushdown. */
    count: Option[CountConfig] = None
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

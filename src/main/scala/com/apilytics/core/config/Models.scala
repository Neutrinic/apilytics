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

/** Schema handling mode.
  *
  * Controls how the OpenAPI schema is used:
  * - strict: Use OpenAPI schema with flattening, typed columns, nested objects as STRING
  * - variant: Single native VARIANT column for schema-free queries (faster than JSON strings)
  */
sealed trait SchemaMode extends Serializable
object SchemaMode {
  /** Use OpenAPI schema strictly with flattening. Nested objects beyond depth become STRING. (Default) */
  case object Strict extends SchemaMode
  /** Return entire response as single native VARIANT column. No schema needed. */
  case object Variant extends SchemaMode
}

/** Checkpoint mode for incremental reads.
  *
  * Controls how progress is tracked across queries:
  * - cursor: Store the last pagination cursor value
  * - timestamp: Store the last record's timestamp
  * - offset: Store the numeric offset position
  */
sealed trait CheckpointMode extends Serializable
object CheckpointMode {
  /** Use pagination cursor value from API responses. */
  case object Cursor extends CheckpointMode
  /** Use timestamp from records for time-based incremental reads.
    * The field referenced by `timestamp-path` must contain ISO-8601 formatted strings
    * (e.g., "2024-01-15T10:30:00Z") so that lexicographic comparison yields correct
    * temporal ordering. Non-ISO formats (Unix epochs, non-zero-padded dates, or
    * timestamps with varying timezone offsets) will produce incorrect results. */
  case object Timestamp extends CheckpointMode
  /** Use numeric offset position. */
  case object Offset extends CheckpointMode
}

/** Response format for HTTP responses.
  *
  * Controls how response bodies are parsed:
  * - json: Full-body JSON (default) - entire response parsed as single JSON
  * - ndjson: Newline-delimited JSON - one JSON object per line
  * - sse: Server-Sent Events - event stream with data fields as JSON
  */
sealed trait ResponseFormat extends Serializable
object ResponseFormat {
  /** Full-body JSON response (default). */
  case object Json extends ResponseFormat
  /** Newline-delimited JSON (JSON Lines). One JSON object per line. */
  case object NDJSON extends ResponseFormat
  /** Server-Sent Events. Event stream format with JSON data fields. */
  case object SSE extends ResponseFormat
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
    /** How many Arrow batches the reader may hold in flight ahead of Spark.
      *
      * Peak reader memory is roughly `prefetch-batches * arrow-batch-size` records,
      * so this is the second half of the memory knob. Raising it trades memory for
      * tolerance of slow/bursty APIs; 1 minimises memory at the cost of no overlap
      * between fetching and consuming. Must be >= 1. */
    prefetchBatches: Int = 2,
    /** When true, empty/null arrays emit one row with null element (OUTER semantics).
      * When false (default), empty arrays produce no rows (INNER semantics). */
    explodeOuter: Boolean = false,
    /** Schema handling mode. Controls how OpenAPI schema is used.
      *
      * - strict: Use OpenAPI schema with flattening (default)
      * - variant: Return entire response as single native VARIANT column */
    mode: SchemaMode = SchemaMode.Strict
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
    responseCache: ResponseCacheConfig = ResponseCacheConfig(),
    /** Response format. Controls how HTTP response bodies are parsed.
      *
      * - json (default): Full-body JSON
      * - ndjson: Newline-delimited JSON (one object per line)
      * - sse: Server-Sent Events */
    responseFormat: ResponseFormat = ResponseFormat.Json
)

/** Configuration for incremental/checkpoint reads.
  *
  * When enabled, the last pagination state (cursor, timestamp, or offset) is persisted
  * to disk after each query. Subsequent queries resume from where the previous one stopped,
  * enabling incremental data loads without re-fetching.
  */
final case class CheckpointConfig(
    /** Enable checkpoint persistence. */
    enabled: Boolean = false,
    /** Directory for checkpoint files. Supports local paths, HDFS, and S3. */
    path: String = "",
    /** How to track progress through the data. */
    mode: CheckpointMode = CheckpointMode.Cursor,
    /** JSON pointer to extract timestamp from records (for timestamp mode).
      * e.g., "/updated_at" or "/created_at".
      *
      * The referenced field must contain ISO-8601 formatted strings
      * (e.g., "2024-01-15T10:30:00Z"). The max timestamp per page is determined
      * by lexicographic comparison, which only yields correct temporal ordering
      * for ISO-8601 with fixed-width components and consistent timezone (UTC). */
    timestampPath: Option[String] = None,
    /** Query parameter to filter by timestamp (for timestamp mode).
      * e.g., "since" or "updated_after" */
    timestampParam: Option[String] = None
)

final case class FilterConfig(
    param: String,
    column: String,
    operators: List[String]
)

/** Configuration for partitioning to enable parallel reads.
  *
  * Partitions are executed in parallel by Spark executors. Each partition creates
  * its own HTTP client with independent rate limiting. See HttpConfig.rateLimit
  * for guidance on setting appropriate rate limits with partitioned reads.
  *
  * Note: `pushedLimit` (from LIMIT N) applies per-partition, not globally.
  * A query with LIMIT 10 and 5 partitions may fetch up to 50 rows before
  * Spark applies the final limit. */
sealed trait PartitionConfig extends Serializable

object PartitionConfig {

  /** Partition by date range (start/end params). */
  final case class DateRange(
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
  ) extends PartitionConfig

  /** Partition by enum values (one partition per value). */
  final case class Enum(
      /** Query parameter name for enum filtering (e.g., "status"). */
      param: String,
      /** List of enum values to partition by (e.g., ["pending", "shipped"]). */
      values: List[String]
  ) extends PartitionConfig
}

sealed trait JoinStrategy extends Serializable
object JoinStrategy {
  /** For each parent row, fetch child endpoint with substituted path parameter. */
  case object NestedLoop extends JoinStrategy
  /** Fetch child records in batches by collecting parent keys. */
  case object Batch extends JoinStrategy
}

/** Configuration for COUNT(*) aggregation pushdown.
  * @deprecated Use AggregationConfig with function="count" instead.
  */
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

/** Aggregation function type for pushdown. */
sealed trait AggregationFunction extends Serializable
object AggregationFunction {
  case object Count extends AggregationFunction
  case object Sum extends AggregationFunction
  case object Avg extends AggregationFunction
  case object Min extends AggregationFunction
  case object Max extends AggregationFunction
  /** Custom aggregation - matched by name. */
  final case class Custom(name: String) extends AggregationFunction
}

/** Configuration for a single aggregation pushdown.
  *
  * Allows pushing aggregations to API endpoints instead of computing locally.
  * Supports standard SQL aggregates (SUM, AVG, etc.) and custom API-specific functions.
  *
  * Example config:
  * {{{
  * aggregations {
  *   total_amount {
  *     function = "sum"
  *     column = "amount"
  *     endpoint = "/orders/stats"
  *     response-path = "/total"
  *   }
  *   amount_p95 {
  *     function = "custom"
  *     name = "PERCENTILE"
  *     endpoint = "/orders/percentiles"
  *     params { p = "95" }
  *     response-path = "/value"
  *   }
  * }
  * }}}
  */
final case class AggregationConfig(
    /** Aggregation function type (sum, avg, min, max, count, custom). */
    function: AggregationFunction,
    /** Column to aggregate (required for sum, avg, min, max). */
    column: Option[String] = None,
    /** Endpoint to call for this aggregation. */
    endpoint: String,
    /** JSON pointer to extract result from response (e.g., "/total"). */
    responsePath: String,
    /** Additional query parameters to send with the request. */
    params: Map[String, String] = Map.empty
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
    /** Configuration for COUNT(*) aggregation pushdown.
      * @deprecated Use aggregations with function="count" instead. */
    count: Option[CountConfig] = None,
    /** Configurable aggregation pushdown. Maps aggregation name to config. */
    aggregations: Map[String, AggregationConfig] = Map.empty,
    /** Checkpoint configuration for incremental reads. */
    checkpoint: Option[CheckpointConfig] = None
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

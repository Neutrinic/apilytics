package com.apilytics.core.config

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object Loader {

  private val log = LoggerFactory.getLogger(getClass)

  def load(path: String): SourceConfig = {
    val config = ConfigFactory.parseFile(new java.io.File(path)).resolve()
    readSourceConfig(config)
  }

  def load(config: Config): SourceConfig = {
    readSourceConfig(config.resolve())
  }

  private def readSourceConfig(config: Config): SourceConfig = {
    val sc = SourceConfig(
      openapi = config.getString("openapi"),
      auth = readAuth(config.getConfig("auth")),
      pagination = if (config.hasPath("pagination")) readPagination(config.getConfig("pagination"))
                   else PaginationConfig(),
      schema = if (config.hasPath("schema")) readSchema(config.getConfig("schema"))
               else SchemaConfig(),
      http = if (config.hasPath("http")) readHttp(config.getConfig("http"))
             else HttpConfig(maxBackoff = 30.seconds, timeout = 30.seconds),
      tables = if (config.hasPath("tables")) readTables(config.getConfig("tables"))
               else Map.empty,
      baseUrl = optional(config, "base-url"),
      cache = if (config.hasPath("cache")) readCache(config.getConfig("cache"))
              else CacheConfig()
    )

    // Validate: checkpoint with link-header pagination is not supported
    if (sc.pagination.style == PaginationStyle.LinkHeader) {
      sc.tables.foreach { case (name, tc) =>
        tc.checkpoint.foreach { cc =>
          if (cc.enabled && cc.mode != CheckpointMode.Timestamp) {
            throw new IllegalArgumentException(
              s"Table '$name' uses checkpoint mode '${cc.mode}' with link-header pagination. " +
              "Link-header pagination does not emit cursor or offset state for checkpoint. " +
              "Use mode 'timestamp' instead, which tracks record timestamps independently of pagination."
            )
          }
        }
      }
    }

    // Warn when auth credentials are configured over plaintext HTTP
    warnPlaintextCredentials(sc)

    sc
  }

  private def readAuth(config: Config): AuthConfig = {
    val authType = config.getString("type") match {
      case "none"          => AuthType.None
      case "bearer"        => AuthType.Bearer
      case "basic"         => AuthType.Basic
      case "header"        => AuthType.Header
      case "oauth2_client" => AuthType.OAuth2Client
      case other           => throw new IllegalArgumentException(s"Unknown auth type: $other")
    }
    AuthConfig(
      authType = authType,
      token = optional(config, "token"),
      username = optional(config, "username"),
      password = optional(config, "password"),
      headerName = optional(config, "header-name"),
      headerValue = optional(config, "header-value"),
      clientId = optional(config, "client-id"),
      clientSecret = optional(config, "client-secret"),
      tokenUrl = optional(config, "token-url")
    )
  }

  private def readPagination(config: Config): PaginationConfig = {
    val style = if (config.hasPath("style")) config.getString("style") match {
      case "cursor"      => PaginationStyle.Cursor
      case "offset"      => PaginationStyle.Offset
      case "link_header" => PaginationStyle.LinkHeader
      case "none"        => PaginationStyle.None
      case other         => throw new IllegalArgumentException(s"Unknown pagination style: $other")
    } else PaginationStyle.None

    PaginationConfig(
      style = style,
      cursorPath = optional(config, "cursor-path"),
      cursorParam = optional(config, "cursor-param"),
      offsetParam = optional(config, "offset-param"),
      pageSizeParam = optional(config, "page-size-param"),
      maxPageSize = if (config.hasPath("max-page-size")) config.getInt("max-page-size") else 100,
      resultsPath = optional(config, "results-path"),
      maxPages = if (config.hasPath("max-pages")) config.getInt("max-pages") else 1000
    )
  }

  private def readSchema(config: Config): SchemaConfig = {
    SchemaConfig(
      flattenDepth = if (config.hasPath("flatten-depth")) config.getInt("flatten-depth") else 2,
      arrayHandling = if (config.hasPath("array-handling")) config.getString("array-handling") match {
        case "keep_array"    => ArrayHandling.KeepArray
        case "explode_view"  => ArrayHandling.ExplodeView
        case "both"          => ArrayHandling.Both
        case other           => throw new IllegalArgumentException(s"Unknown array handling: $other")
      } else ArrayHandling.KeepArray,
      arrowBatchSize = if (config.hasPath("arrow-batch-size")) config.getInt("arrow-batch-size") else 4096,
      prefetchBatches = if (config.hasPath("prefetch-batches")) {
        val n = config.getInt("prefetch-batches")
        // Queue.bounded rejects a capacity below 1, and the failure would otherwise
        // surface on an executor thread long after config load.
        if (n < 1) throw new IllegalArgumentException(s"prefetch-batches must be >= 1, got: $n")
        n
      } else 2,
      explodeOuter = if (config.hasPath("explode-outer")) config.getBoolean("explode-outer") else false,
      mode = if (config.hasPath("mode")) config.getString("mode") match {
        case "strict"  => SchemaMode.Strict
        case "variant" => SchemaMode.Variant
        case other     => throw new IllegalArgumentException(s"Unknown schema mode: $other. Valid modes: strict, variant")
      } else SchemaMode.Strict
    )
  }

  private def readHttp(config: Config): HttpConfig = {
    HttpConfig(
      maxRetries = if (config.hasPath("max-retries")) config.getInt("max-retries") else 5,
      maxBackoff = if (config.hasPath("max-backoff")) Duration(config.getString("max-backoff")).asInstanceOf[FiniteDuration]
                   else 30.seconds,
      timeout = if (config.hasPath("timeout")) Duration(config.getString("timeout")).asInstanceOf[FiniteDuration]
                else 30.seconds,
      rateLimit = if (config.hasPath("rate-limit")) Some(config.getInt("rate-limit")) else None,
      responseCache = if (config.hasPath("response-cache")) readResponseCache(config.getConfig("response-cache"))
                      else ResponseCacheConfig(),
      responseFormat = if (config.hasPath("response-format")) config.getString("response-format") match {
        case "json"             => ResponseFormat.Json
        case "ndjson" | "jsonl" => ResponseFormat.NDJSON
        case "sse"              => ResponseFormat.SSE
        case other => throw new IllegalArgumentException(
          s"Unknown response format: $other. Valid formats: json, ndjson, sse"
        )
      } else ResponseFormat.Json
    )
  }

  private def readResponseCache(config: Config): ResponseCacheConfig = {
    val backend = if (config.hasPath("backend")) config.getString("backend") match {
      case "memory" => ResponseCacheBackend.Memory
      case other    => throw new IllegalArgumentException(s"Unknown response cache backend: $other")
    } else ResponseCacheBackend.Memory

    ResponseCacheConfig(
      enabled = if (config.hasPath("enabled")) config.getBoolean("enabled") else false,
      backend = backend,
      ttl = if (config.hasPath("ttl")) Duration(config.getString("ttl")).asInstanceOf[FiniteDuration]
            else 5.minutes,
      maxEntries = if (config.hasPath("max-entries")) config.getInt("max-entries") else 1000
    )
  }

  private def readTables(config: Config): Map[String, TableConfig] = {
    config.root().keySet().asScala.map { name =>
      val tc = config.getConfig(name)
      val tableConfig = TableConfig(
        endpoint = tc.getString("endpoint"),
        dataPath = optional(tc, "data-path"),
        pagination =
          if (tc.hasPath("pagination")) Some(readPagination(tc.getConfig("pagination"))) else None,
        filters = if (tc.hasPath("filters")) {
          tc.getConfigList("filters").asScala.toList.map { fc =>
            FilterConfig(
              param = fc.getString("param"),
              column = fc.getString("column"),
              operators = fc.getStringList("operators").asScala.toList
            )
          }
        } else Nil,
        parentTable = optional(tc, "parent-table"),
        parentKey = optional(tc, "parent-key"),
        joinStrategy = if (tc.hasPath("join-strategy")) tc.getString("join-strategy") match {
          case "nested_loop" => Some(JoinStrategy.NestedLoop)
          case "batch"       => Some(JoinStrategy.Batch)
          case other         => throw new IllegalArgumentException(s"Unknown join strategy: $other")
        } else None,
        partition = if (tc.hasPath("partition")) Some(readPartition(tc.getConfig("partition"))) else None,
        batchParam = optional(tc, "batch-param"),
        batchSize = if (tc.hasPath("batch-size")) tc.getInt("batch-size") else 100,
        batchSeparator = if (tc.hasPath("batch-separator")) tc.getString("batch-separator") else ",",
        childKeyField = optional(tc, "child-key-field"),
        count = if (tc.hasPath("count")) Some(readCount(tc.getConfig("count"))) else None,
        aggregations = if (tc.hasPath("aggregations")) readAggregations(tc.getConfig("aggregations"))
                       else Map.empty,
        checkpoint = if (tc.hasPath("checkpoint")) Some(readCheckpoint(tc.getConfig("checkpoint"))) else None
      )
      
      // Validate batch join configuration
      if (tableConfig.joinStrategy.contains(JoinStrategy.Batch)) {
        if (tableConfig.batchParam.isEmpty) {
          throw new IllegalArgumentException(
            s"Table '$name' uses batch join strategy but missing 'batch-param'. " +
            "Batch joins require a query parameter for bulk lookups (e.g., batch-param = \"ids\")"
          )
        }
        if (tableConfig.endpoint.contains("{")) {
          throw new IllegalArgumentException(
            s"Table '$name' uses batch join strategy but endpoint contains path template '${tableConfig.endpoint}'. " +
            "Batch joins require a base endpoint without path parameters (e.g., endpoint = \"/orders\", not \"/orders/{id}/items\"). " +
            s"The batch IDs are passed via query parameter '${tableConfig.batchParam.get}', not URL substitution."
          )
        }
      }

      // Validate count config requires either endpoint or param
      tableConfig.count.foreach { countConfig =>
        if (countConfig.endpoint.isEmpty && countConfig.param.isEmpty) {
          throw new IllegalArgumentException(
            s"Table '$name' has count config but missing 'endpoint' or 'param'. " +
            "Count pushdown requires either a dedicated count endpoint (e.g., endpoint = \"/items/count\") " +
            "or a query parameter (e.g., param = \"include\", param-value = \"total_count\")."
          )
        }
      }

      name -> tableConfig
    }.toMap
  }

  private def readCache(config: Config): CacheConfig = {
    CacheConfig(
      enabled = if (config.hasPath("enabled")) config.getBoolean("enabled") else false,
      ttl = if (config.hasPath("ttl")) Some(Duration(config.getString("ttl")).asInstanceOf[FiniteDuration])
            else None,
      directory = optional(config, "directory")
    )
  }

  private def readPartition(config: Config): PartitionConfig = {
    val partitionType = if (config.hasPath("type")) config.getString("type") else "date-range"

    partitionType match {
      case "date-range" =>
        val range = Duration(config.getString("range")) match {
          case fd: FiniteDuration => fd
          case _ => throw new IllegalArgumentException(
            s"partition.range must be a finite duration (got '${config.getString("range")}')"
          )
        }
        PartitionConfig.DateRange(
          column = config.getString("column"),
          range = range,
          startParam = config.getString("start-param"),
          endParam = config.getString("end-param"),
          format = if (config.hasPath("format")) config.getString("format")
                   else "yyyy-MM-dd'T'HH:mm:ss'Z'"
        )

      case "enum" =>
        val param = if (config.hasPath("param")) config.getString("param")
                    else throw new IllegalArgumentException(
                      "Enum partition requires 'param' (query parameter name for filtering)"
                    )
        val values = if (config.hasPath("values")) config.getStringList("values").asScala.toList
                     else throw new IllegalArgumentException(
                       "Enum partition requires 'values' (list of enum values to partition by)"
                     )
        if (values.isEmpty) {
          throw new IllegalArgumentException(
            "Enum partition requires non-empty 'values' list"
          )
        }
        PartitionConfig.Enum(
          param = param,
          values = values
        )

      case other =>
        throw new IllegalArgumentException(s"Unknown partition type: $other")
    }
  }

  private def readCount(config: Config): CountConfig = {
    CountConfig(
      endpoint = optional(config, "endpoint"),
      param = optional(config, "param"),
      paramValue = if (config.hasPath("param-value")) config.getString("param-value") else "true",
      responsePath = config.getString("response-path")
    )
  }

  private def readAggregations(config: Config): Map[String, AggregationConfig] = {
    config.root().keySet().asScala.map { name =>
      val ac = config.getConfig(name)
      val function = ac.getString("function") match {
        case "count"  => AggregationFunction.Count
        case "sum"    => AggregationFunction.Sum
        case "avg"    => AggregationFunction.Avg
        case "min"    => AggregationFunction.Min
        case "max"    => AggregationFunction.Max
        case "custom" =>
          val customName = if (ac.hasPath("name")) ac.getString("name")
                           else throw new IllegalArgumentException(
                             s"Aggregation '$name' has function=custom but missing 'name' field"
                           )
          AggregationFunction.Custom(customName)
        case other => throw new IllegalArgumentException(s"Unknown aggregation function: $other")
      }

      val aggConfig = AggregationConfig(
        function = function,
        column = optional(ac, "column"),
        endpoint = ac.getString("endpoint"),
        responsePath = ac.getString("response-path"),
        params = if (ac.hasPath("params")) {
          val paramsConfig = ac.getConfig("params")
          paramsConfig.root().keySet().asScala.map { key =>
            key -> paramsConfig.getString(key)
          }.toMap
        } else Map.empty,
      )

      // Validate column is required for sum/avg/min/max
      function match {
        case AggregationFunction.Sum | AggregationFunction.Avg |
             AggregationFunction.Min | AggregationFunction.Max if aggConfig.column.isEmpty =>
          throw new IllegalArgumentException(
            s"Aggregation '$name' with function=${ac.getString("function")} requires 'column' field"
          )
        case _ => // OK
      }

      name -> aggConfig
    }.toMap
  }

  private def readCheckpoint(config: Config): CheckpointConfig = {
    val mode = if (config.hasPath("mode")) config.getString("mode") match {
      case "cursor"    => CheckpointMode.Cursor
      case "timestamp" => CheckpointMode.Timestamp
      case "offset"    => CheckpointMode.Offset
      case other       => throw new IllegalArgumentException(
        s"Unknown checkpoint mode: $other. Valid modes: cursor, timestamp, offset"
      )
    } else CheckpointMode.Cursor

    val cc = CheckpointConfig(
      enabled = if (config.hasPath("enabled")) config.getBoolean("enabled") else false,
      path = if (config.hasPath("path")) config.getString("path") else "",
      mode = mode,
      timestampPath = optional(config, "timestamp-path"),
      timestampParam = optional(config, "timestamp-param")
    )

    // Validate: enabled checkpoint requires path
    if (cc.enabled && cc.path.isEmpty) {
      throw new IllegalArgumentException(
        "Checkpoint is enabled but 'path' is not configured. " +
        "Specify a directory for checkpoint files (e.g., path = \"/tmp/apilytics/checkpoints\")"
      )
    }

    // Validate: timestamp mode requires timestamp-path and timestamp-param
    if (cc.enabled && cc.mode == CheckpointMode.Timestamp) {
      if (cc.timestampPath.isEmpty) {
        throw new IllegalArgumentException(
          "Checkpoint mode 'timestamp' requires 'timestamp-path' (JSON pointer to timestamp field, e.g., \"/updated_at\")"
        )
      }
      if (cc.timestampParam.isEmpty) {
        throw new IllegalArgumentException(
          "Checkpoint mode 'timestamp' requires 'timestamp-param' (query parameter for filtering, e.g., \"since\")"
        )
      }
    }

    cc
  }

  /** Check if auth credentials would be sent over plaintext HTTP and warn.
    * This catches accidental `http://` typos that would leak Bearer tokens,
    * Basic Auth credentials, or API keys in plaintext.
    */
  private[config] def warnPlaintextCredentials(sc: SourceConfig): List[String] = {
    if (sc.auth.authType == AuthType.None) return Nil

    val warnings = List.newBuilder[String]

    sc.baseUrl.foreach { url =>
      if (url.toLowerCase.startsWith("http://")) {
        val msg = s"SECURITY: base-url uses plaintext HTTP ($url). " +
          "Credentials will be sent unencrypted. Use https:// instead."
        log.warn(msg)
        warnings += msg
      }
    }

    sc.auth.tokenUrl.foreach { url =>
      if (url.toLowerCase.startsWith("http://")) {
        val msg = s"SECURITY: token-url uses plaintext HTTP ($url). " +
          "OAuth2 client credentials will be sent unencrypted. Use https:// instead."
        log.warn(msg)
        warnings += msg
      }
    }

    warnings.result()
  }

  private def optional(config: Config, path: String): Option[String] =
    if (config.hasPath(path)) Some(config.getString(path)) else None
}

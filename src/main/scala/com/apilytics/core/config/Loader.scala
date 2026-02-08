package com.apilytics.core.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object Loader {

  def load(path: String): SourceConfig = {
    val config = ConfigFactory.parseFile(new java.io.File(path)).resolve()
    readSourceConfig(config)
  }

  def load(config: Config): SourceConfig = {
    readSourceConfig(config.resolve())
  }

  private def readSourceConfig(config: Config): SourceConfig = {
    SourceConfig(
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
      baseUrl = optional(config, "base-url")
    )
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
      explodeOuter = if (config.hasPath("explode-outer")) config.getBoolean("explode-outer") else false
    )
  }

  private def readHttp(config: Config): HttpConfig = {
    HttpConfig(
      maxRetries = if (config.hasPath("max-retries")) config.getInt("max-retries") else 5,
      maxBackoff = if (config.hasPath("max-backoff")) Duration(config.getString("max-backoff")).asInstanceOf[FiniteDuration]
                   else 30.seconds,
      timeout = if (config.hasPath("timeout")) Duration(config.getString("timeout")).asInstanceOf[FiniteDuration]
                else 30.seconds,
      rateLimit = if (config.hasPath("rate-limit")) Some(config.getInt("rate-limit")) else None
    )
  }

  private def readTables(config: Config): Map[String, TableConfig] = {
    config.root().keySet().asScala.map { name =>
      val tc = config.getConfig(name)
      name -> TableConfig(
        endpoint = tc.getString("endpoint"),
        dataPath = optional(tc, "data-path"),
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
          case other         => throw new IllegalArgumentException(s"Unknown join strategy: $other")
        } else None
      )
    }.toMap
  }

  private def optional(config: Config, path: String): Option[String] =
    if (config.hasPath(path)) Some(config.getString(path)) else None
}

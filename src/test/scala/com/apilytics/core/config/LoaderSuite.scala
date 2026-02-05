package com.apilytics.core.config

import com.typesafe.config.ConfigFactory
import munit.FunSuite

class LoaderSuite extends FunSuite {

  test("load minimal config with bearer auth") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "https://example.com/openapi.json"
        |auth {
        |  type = bearer
        |  token = "abc123"
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.openapi, "https://example.com/openapi.json")
    assertEquals(result.auth.authType, AuthType.Bearer)
    assertEquals(result.auth.token, Some("abc123"))
    assertEquals(result.pagination.style, PaginationStyle.None)
    assertEquals(result.schema.flattenDepth, 2)
  }

  test("load full config with all sections") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth {
        |  type = basic
        |  username = "user"
        |  password = "pass"
        |}
        |pagination {
        |  style = cursor
        |  cursor-path = "/meta/next_cursor"
        |  cursor-param = "cursor"
        |  page-size-param = "per_page"
        |  max-page-size = 50
        |}
        |schema {
        |  flatten-depth = 3
        |  array-handling = explode_view
        |}
        |http {
        |  max-retries = 3
        |  max-backoff = "10 seconds"
        |  timeout = "5 seconds"
        |}
        |tables {
        |  users {
        |    endpoint = "/users"
        |    data-path = "/data"
        |    filters = [
        |      { param = "email", column = "email", operators = ["eq"] }
        |    ]
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.auth.authType, AuthType.Basic)
    assertEquals(result.auth.username, Some("user"))
    assertEquals(result.pagination.style, PaginationStyle.Cursor)
    assertEquals(result.pagination.maxPageSize, 50)
    assertEquals(result.schema.flattenDepth, 3)
    assertEquals(result.schema.arrayHandling, ArrayHandling.ExplodeView)
    assertEquals(result.http.maxRetries, 3)
    assertEquals(result.tables.size, 1)
    val users = result.tables("users")
    assertEquals(users.endpoint, "/users")
    assertEquals(users.dataPath, Some("/data"))
    assertEquals(users.filters.size, 1)
    assertEquals(users.filters.head.param, "email")
  }

  test("unknown auth type throws") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = "magic" }
        |""".stripMargin)

    intercept[IllegalArgumentException] {
      Loader.load(config)
    }
  }

  test("header auth config") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth {
        |  type = header
        |  header-name = "X-Api-Key"
        |  header-value = "secret"
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.auth.authType, AuthType.Header)
    assertEquals(result.auth.headerName, Some("X-Api-Key"))
    assertEquals(result.auth.headerValue, Some("secret"))
  }

  test("all pagination styles parse correctly") {
    def withStyle(style: String) = ConfigFactory.parseString(
      s"""
         |openapi = "spec.json"
         |auth { type = bearer, token = "t" }
         |pagination { style = $style }
         |""".stripMargin)

    assertEquals(Loader.load(withStyle("offset")).pagination.style, PaginationStyle.Offset)
    assertEquals(Loader.load(withStyle("link_header")).pagination.style, PaginationStyle.LinkHeader)
    assertEquals(Loader.load(withStyle("none")).pagination.style, PaginationStyle.None)
  }

  test("arrow-batch-size defaults to 4096") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.schema.arrowBatchSize, 4096)
  }

  test("arrow-batch-size can be configured") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |schema {
        |  arrow-batch-size = 1024
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.schema.arrowBatchSize, 1024)
  }

  test("results-path and max-pages default values") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |pagination { style = offset }
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.pagination.resultsPath, None)
    assertEquals(result.pagination.maxPages, 1000)
  }

  test("results-path and max-pages can be configured") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |pagination {
        |  style = offset
        |  results-path = "/results"
        |  max-pages = 50
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.pagination.resultsPath, Some("/results"))
    assertEquals(result.pagination.maxPages, 50)
  }
}

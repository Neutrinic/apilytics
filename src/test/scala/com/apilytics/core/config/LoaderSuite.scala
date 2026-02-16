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

  test("response-cache defaults to disabled") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.http.responseCache.enabled, false)
    assertEquals(result.http.responseCache.backend, ResponseCacheBackend.Memory)
  }

  test("response-cache can be configured") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |http {
        |  response-cache {
        |    enabled = true
        |    backend = memory
        |    ttl = "10 minutes"
        |    max-entries = 500
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.http.responseCache.enabled, true)
    assertEquals(result.http.responseCache.backend, ResponseCacheBackend.Memory)
    assertEquals(result.http.responseCache.ttl.toMinutes, 10L)
    assertEquals(result.http.responseCache.maxEntries, 500)
  }

  test("aggregations config with standard functions") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  orders {
        |    endpoint = "/orders"
        |    aggregations {
        |      total_amount {
        |        function = sum
        |        column = "amount"
        |        endpoint = "/orders/stats"
        |        response-path = "/total"
        |      }
        |      avg_amount {
        |        function = avg
        |        column = "amount"
        |        endpoint = "/orders/stats"
        |        response-path = "/average"
        |      }
        |      min_price {
        |        function = min
        |        column = "price"
        |        endpoint = "/orders/stats"
        |        response-path = "/min_price"
        |      }
        |      max_price {
        |        function = max
        |        column = "price"
        |        endpoint = "/orders/stats"
        |        response-path = "/max_price"
        |      }
        |      order_count {
        |        function = count
        |        endpoint = "/orders/count"
        |        response-path = "/count"
        |      }
        |    }
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    val orders = result.tables("orders")
    assertEquals(orders.aggregations.size, 5)

    val sumAgg = orders.aggregations("total_amount")
    assertEquals(sumAgg.function, AggregationFunction.Sum)
    assertEquals(sumAgg.column, Some("amount"))
    assertEquals(sumAgg.endpoint, "/orders/stats")
    assertEquals(sumAgg.responsePath, "/total")

    val avgAgg = orders.aggregations("avg_amount")
    assertEquals(avgAgg.function, AggregationFunction.Avg)

    val minAgg = orders.aggregations("min_price")
    assertEquals(minAgg.function, AggregationFunction.Min)

    val maxAgg = orders.aggregations("max_price")
    assertEquals(maxAgg.function, AggregationFunction.Max)

    val countAgg = orders.aggregations("order_count")
    assertEquals(countAgg.function, AggregationFunction.Count)
    assertEquals(countAgg.column, None)
  }

  test("aggregations config with custom function") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  orders {
        |    endpoint = "/orders"
        |    aggregations {
        |      amount_p95 {
        |        function = custom
        |        name = "PERCENTILE"
        |        endpoint = "/orders/percentiles"
        |        response-path = "/p95"
        |        params {
        |          percentile = "95"
        |          column = "amount"
        |        }
        |      }
        |    }
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    val orders = result.tables("orders")
    val pctAgg = orders.aggregations("amount_p95")

    pctAgg.function match {
      case AggregationFunction.Custom(name) =>
        assertEquals(name, "PERCENTILE")
      case other =>
        fail(s"Expected Custom function, got $other")
    }
    assertEquals(pctAgg.params, Map("percentile" -> "95", "column" -> "amount"))
  }

  test("aggregation with custom function requires name") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  orders {
        |    endpoint = "/orders"
        |    aggregations {
        |      custom_agg {
        |        function = custom
        |        endpoint = "/orders/custom"
        |        response-path = "/result"
        |      }
        |    }
        |  }
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      Loader.load(config)
    }
    assert(ex.getMessage.contains("missing 'name'"))
  }

  test("sum/avg/min/max aggregation requires column") {
    val functions = List("sum", "avg", "min", "max")

    functions.foreach { fn =>
      val config = ConfigFactory.parseString(
        s"""
           |openapi = "spec.json"
           |auth { type = bearer, token = "t" }
           |tables {
           |  orders {
           |    endpoint = "/orders"
           |    aggregations {
           |      test_agg {
           |        function = $fn
           |        endpoint = "/orders/stats"
           |        response-path = "/value"
           |      }
           |    }
           |  }
           |}
           |""".stripMargin)

      val ex = intercept[IllegalArgumentException] {
        Loader.load(config)
      }
      assert(ex.getMessage.contains("requires 'column'"), s"$fn should require column")
    }
  }

  // ==========================================================================
  // Schema mode tests (#137)
  // ==========================================================================

  test("schema mode defaults to strict") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.schema.mode, SchemaMode.Strict)
  }

  test("schema mode can be set to variant") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |schema {
        |  mode = variant
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.schema.mode, SchemaMode.Variant)
  }

  test("schema mode can be set to lenient") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |schema {
        |  mode = lenient
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.schema.mode, SchemaMode.Lenient)
  }

  test("schema mode can be set to infer") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |schema {
        |  mode = infer
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.schema.mode, SchemaMode.Infer)
  }

  test("unknown schema mode throws") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |schema {
        |  mode = unknown
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      Loader.load(config)
    }
    assert(ex.getMessage.contains("Unknown schema mode"))
  }
}

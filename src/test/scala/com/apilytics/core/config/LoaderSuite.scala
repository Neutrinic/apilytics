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

  test("prefetch-batches defaults to 2") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.schema.prefetchBatches, 2)
  }

  test("prefetch-batches can be configured") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |schema {
        |  prefetch-batches = 8
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.schema.prefetchBatches, 8)
  }

  test("prefetch-batches below 1 is rejected at load time") {
    def withPrefetch(n: Int) = ConfigFactory.parseString(
      s"""
         |openapi = "spec.json"
         |auth { type = bearer, token = "t" }
         |schema {
         |  prefetch-batches = $n
         |}
         |""".stripMargin)

    // Queue.bounded would otherwise fail on an executor thread, far from the cause.
    List(0, -1).foreach { n =>
      val ex = intercept[IllegalArgumentException](Loader.load(withPrefetch(n)))
      assert(ex.getMessage.contains("prefetch-batches must be >= 1"), ex.getMessage)
    }
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

  // ==========================================================================
  // Checkpoint config tests (#49)
  // ==========================================================================

  test("checkpoint config parses cursor mode") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    checkpoint {
        |      enabled = true
        |      path = "/tmp/checkpoints"
        |      mode = cursor
        |    }
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    val cp = result.tables("events").checkpoint.get
    assert(cp.enabled)
    assertEquals(cp.path, "/tmp/checkpoints")
    assertEquals(cp.mode, CheckpointMode.Cursor)
  }

  test("checkpoint config parses timestamp mode") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    checkpoint {
        |      enabled = true
        |      path = "/tmp/checkpoints"
        |      mode = timestamp
        |      timestamp-path = "/updated_at"
        |      timestamp-param = "since"
        |    }
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    val cp = result.tables("events").checkpoint.get
    assertEquals(cp.mode, CheckpointMode.Timestamp)
    assertEquals(cp.timestampPath, Some("/updated_at"))
    assertEquals(cp.timestampParam, Some("since"))
  }

  test("checkpoint config parses offset mode") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    checkpoint {
        |      enabled = true
        |      path = "/tmp/checkpoints"
        |      mode = offset
        |    }
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    val cp = result.tables("events").checkpoint.get
    assertEquals(cp.mode, CheckpointMode.Offset)
  }

  test("checkpoint defaults to cursor mode") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    checkpoint {
        |      enabled = true
        |      path = "/tmp/checkpoints"
        |    }
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    val cp = result.tables("events").checkpoint.get
    assertEquals(cp.mode, CheckpointMode.Cursor)
  }

  test("checkpoint requires path when enabled") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    checkpoint {
        |      enabled = true
        |    }
        |  }
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      Loader.load(config)
    }
    assert(ex.getMessage.contains("path"))
  }

  test("checkpoint timestamp mode requires timestamp-path") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    checkpoint {
        |      enabled = true
        |      path = "/tmp/checkpoints"
        |      mode = timestamp
        |      timestamp-param = "since"
        |    }
        |  }
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      Loader.load(config)
    }
    assert(ex.getMessage.contains("timestamp-path"))
  }

  test("checkpoint timestamp mode requires timestamp-param") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    checkpoint {
        |      enabled = true
        |      path = "/tmp/checkpoints"
        |      mode = timestamp
        |      timestamp-path = "/updated_at"
        |    }
        |  }
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      Loader.load(config)
    }
    assert(ex.getMessage.contains("timestamp-param"))
  }

  test("unknown checkpoint mode throws") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    checkpoint {
        |      enabled = true
        |      path = "/tmp/checkpoints"
        |      mode = unknown
        |    }
        |  }
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      Loader.load(config)
    }
    assert(ex.getMessage.contains("Unknown checkpoint mode"))
  }

  test("checkpoint cursor mode with link-header pagination throws") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |pagination { style = link_header }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    checkpoint {
        |      enabled = true
        |      path = "/tmp/checkpoints"
        |      mode = "cursor"
        |    }
        |  }
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      Loader.load(config)
    }
    assert(ex.getMessage.contains("link-header pagination"), ex.getMessage)
  }

  test("checkpoint timestamp mode with link-header pagination is allowed") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |pagination { style = link_header }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    checkpoint {
        |      enabled = true
        |      path = "/tmp/checkpoints"
        |      mode = "timestamp"
        |      timestamp-path = "/updated_at"
        |      timestamp-param = "since"
        |    }
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.tables("events").checkpoint.get.mode, CheckpointMode.Timestamp)
  }

  test("table without checkpoint has None") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.tables("events").checkpoint, None)
  }

  // ==========================================================================
  // Plaintext HTTP credential warning tests (#163)
  // ==========================================================================

  test("warns when base-url uses plaintext HTTP with auth") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |base-url = "http://api.example.com"
        |""".stripMargin)

    val result = Loader.load(config)
    val warnings = Loader.warnPlaintextCredentials(result)
    assertEquals(warnings.size, 1)
    assert(warnings.head.contains("base-url"))
    assert(warnings.head.contains("http://api.example.com"))
  }

  test("warns when token-url uses plaintext HTTP") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth {
        |  type = oauth2_client
        |  client-id = "id"
        |  client-secret = "secret"
        |  token-url = "http://auth.example.com/token"
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    val warnings = Loader.warnPlaintextCredentials(result)
    assertEquals(warnings.size, 1)
    assert(warnings.head.contains("token-url"))
    assert(warnings.head.contains("http://auth.example.com/token"))
  }

  test("warns for both base-url and token-url over HTTP") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth {
        |  type = oauth2_client
        |  client-id = "id"
        |  client-secret = "secret"
        |  token-url = "http://auth.example.com/token"
        |}
        |base-url = "http://api.example.com"
        |""".stripMargin)

    val result = Loader.load(config)
    val warnings = Loader.warnPlaintextCredentials(result)
    assertEquals(warnings.size, 2)
  }

  test("no warning when URLs use HTTPS") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |base-url = "https://api.example.com"
        |""".stripMargin)

    val result = Loader.load(config)
    val warnings = Loader.warnPlaintextCredentials(result)
    assertEquals(warnings.size, 0)
  }

  test("no warning when auth type is none") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = none }
        |base-url = "http://api.example.com"
        |""".stripMargin)

    val result = Loader.load(config)
    val warnings = Loader.warnPlaintextCredentials(result)
    assertEquals(warnings.size, 0)
  }

  test("no warning when base-url is not set") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |""".stripMargin)

    val result = Loader.load(config)
    val warnings = Loader.warnPlaintextCredentials(result)
    assertEquals(warnings.size, 0)
  }

  // --- Unknown keys ---
  //
  // Every field is read with hasPath, so a typo is not an error — it is an absent key
  // and a silent default. These pin the check that turns that into a failure.

  private def loadString(hocon: String) = Loader.load(ConfigFactory.parseString(hocon))

  test("a misspelled auth key fails instead of silently disabling auth") {
    // The motivating case: `tokn` leaves token = None, so a bearer-auth source makes
    // unauthenticated requests against a live API with nothing explaining why.
    val e = intercept[IllegalArgumentException] {
      loadString("""
        |openapi = "https://example.com/openapi.json"
        |auth { type = bearer, tokn = "secret" }
        |""".stripMargin)
    }
    assert(e.getMessage.contains("auth.tokn"), e.getMessage)
  }

  test("a misspelled pagination key is reported with its full path") {
    val e = intercept[IllegalArgumentException] {
      loadString("""
        |openapi = "https://example.com/openapi.json"
        |auth { type = none }
        |pagination { style = offset, offest-param = "offset" }
        |""".stripMargin)
    }
    assert(e.getMessage.contains("pagination.offest-param"), e.getMessage)
  }

  test("unknown keys inside a table are found") {
    val e = intercept[IllegalArgumentException] {
      loadString("""
        |openapi = "https://example.com/openapi.json"
        |auth { type = none }
        |tables { issues { endpoint = "/issues", datapath = "/items" } }
        |""".stripMargin)
    }
    assert(e.getMessage.contains("tables.issues.datapath"), e.getMessage)
  }

  test("every unknown key is reported, not just the first") {
    val e = intercept[IllegalArgumentException] {
      loadString("""
        |openapi = "https://example.com/openapi.json"
        |auth { type = none, tokn = "x" }
        |schema { flatten-dept = 3 }
        |""".stripMargin)
    }
    assert(e.getMessage.contains("auth.tokn"), e.getMessage)
    assert(e.getMessage.contains("schema.flatten-dept"), e.getMessage)
  }

  test("user-named sections keep accepting arbitrary names") {
    // Table names, aggregation names and query parameters are chosen by the user or
    // dictated by the API, so they must not be checked against a fixed key set.
    val cfg = loadString("""
      |openapi = "https://example.com/openapi.json"
      |auth { type = none }
      |tables {
      |  whatever_name_i_like {
      |    endpoint = "/x"
      |    aggregations {
      |      my_count {
      |        function = "count"
      |        endpoint = "/x/count"
      |        response-path = "/total"
      |        params { any_api_param = "1", another = "2" }
      |      }
      |    }
      |  }
      |}
      |""".stripMargin)

    assertEquals(cfg.tables.keySet, Set("whatever_name_i_like"))
    assertEquals(cfg.tables("whatever_name_i_like").aggregations.keySet, Set("my_count"))
  }

  test("filters are checked inside the list") {
    val e = intercept[IllegalArgumentException] {
      loadString("""
        |openapi = "https://example.com/openapi.json"
        |auth { type = none }
        |tables { issues {
        |  endpoint = "/issues"
        |  filters = [ { param = "state", colum = "state", operators = ["="] } ]
        |} }
        |""".stripMargin)
    }
    assert(e.getMessage.contains("colum"), e.getMessage)
  }

  test("a fully-specified config reports nothing") {
    // Guards against the schema being so narrow that valid configs are rejected.
    assertEquals(
      Loader.unknownKeys(ConfigFactory.parseString("""
        |openapi = "s.yaml"
        |base-url = "https://api.example.com"
        |auth { type = bearer, token = "t" }
        |pagination { style = offset, offset-param = "o", page-size-param = "l"
        |             max-page-size = 50, results-path = "/r", max-pages = 10 }
        |schema { flatten-depth = 1, array-handling = keep_array, arrow-batch-size = 512
        |         prefetch-batches = 4, explode-outer = true, mode = strict }
        |http { max-retries = 2, max-backoff = "10s", timeout = "5s", rate-limit = 3
        |       response-format = json
        |       response-cache { enabled = true, backend = memory, ttl = "1m", max-entries = 10 } }
        |cache { enabled = true, ttl = "1h", directory = "/tmp/c" }
        |tables { t {
        |  endpoint = "/t", data-path = "/d"
        |  pagination { style = cursor, cursor-path = "/next", cursor-param = "c" }
        |  filters = [ { param = "p", column = "c", operators = ["="] } ]
        |  parent-table = "p", parent-key = "id", join-strategy = "batch"
        |  batch-param = "ids", batch-size = 10, batch-separator = ";", child-key-field = "pid"
        |  partition { type = "date-range", column = "at", range = "1d"
        |              start-param = "s", end-param = "e", format = "yyyy-MM-dd" }
        |  count { endpoint = "/t/count", param = "c", param-value = "1", response-path = "/n" }
        |  checkpoint { enabled = true, path = "/tmp/cp", mode = timestamp
        |               timestamp-path = "/at", timestamp-param = "since" }
        |} }
        |""".stripMargin)),
      Nil
    )
  }

  // --- Spec location resolution ---
  //
  // A spec bundled next to its config must be findable wherever the pair is mounted,
  // so relative paths anchor to the config's directory rather than the process CWD.

  private val configDir = Some(new java.io.File("/etc/apilytics"))

  test("relative spec path resolves against the config's directory") {
    assertEquals(
      Loader.resolveSpecLocation("pokeapi-spec.yaml", configDir),
      new java.io.File("/etc/apilytics/pokeapi-spec.yaml").getPath
    )
  }

  test("absolute spec paths are left alone") {
    val absolute = new java.io.File("/srv/specs/api.yaml").getAbsolutePath
    assertEquals(Loader.resolveSpecLocation(absolute, configDir), absolute)
  }

  test("a unix absolute path stays absolute on every platform") {
    // File.isAbsolute calls "/opt/..." relative on Windows, which would join a
    // Linux-authored config's spec path onto the config directory. Configs are written
    // once and run on both, so the leading slash has to be honoured either way.
    val unix = "/opt/apilytics/examples/slack/slack-openapi.json"
    assertEquals(Loader.resolveSpecLocation(unix, configDir), unix)
  }

  test("URLs are left alone") {
    // A relative-looking URL would otherwise be prefixed into a nonexistent local path.
    for (url <- List(
           "https://example.com/openapi.json",
           "http://example.com/openapi.json",
           "s3://bucket/openapi.json",
           "hdfs://namenode/specs/openapi.json",
           "file:///srv/specs/api.yaml",
           "classpath:openapi.json"
         )) assertEquals(Loader.resolveSpecLocation(url, configDir), url, s"rewrote $url")
  }

  test("a Windows drive letter is not mistaken for a URL scheme") {
    // "C:" matches a naive scheme regex; requiring 2+ chars before the colon excludes it.
    //
    // What the path then means is genuinely platform-dependent: on Windows it is an
    // absolute path and passes through, while on Linux it is an ordinary — if oddly
    // named — relative file and anchors to the config directory. The bug this guards
    // against is neither of those: returning it unanchored on a platform where it is
    // relative, because it was read as a URL.
    val windows  = "C:\\specs\\api.yaml"
    val resolved = Loader.resolveSpecLocation(windows, configDir)

    if (new java.io.File(windows).isAbsolute) assertEquals(resolved, windows)
    else assertEquals(resolved, new java.io.File(configDir.get, windows).getPath)
  }

  test("load() anchors a bundled spec to the config file it came from") {
    // The end-to-end path: the same pairing the shipped examples rely on.
    val dir = java.nio.file.Files.createTempDirectory("apilytics-spec-resolve")
    val conf = dir.resolve("source.conf")
    java.nio.file.Files.write(
      conf,
      """openapi = "bundled-spec.yaml"
        |auth { type = bearer, token = "t" }
        |""".stripMargin.getBytes("UTF-8")
    )

    val result = Loader.load(conf.toString)
    assertEquals(result.openapi, dir.resolve("bundled-spec.yaml").toFile.getPath)
  }
}

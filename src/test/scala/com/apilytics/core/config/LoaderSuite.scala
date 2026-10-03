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

  // --- Partition / pagination parameter collisions ---
  //
  // Both write the same query parameter and the paginator wins, so every partition walks
  // the endpoint from its own first page to the end instead of covering a slice. Nothing
  // errors — the query just returns the whole dataset once per partition.

  test("enum partitioning on the pagination offset parameter is rejected") {
    // The exact shape that returned 5404 rows for 1351 distinct records against PokeAPI,
    // with four times the API calls charged against the rate limit.
    val e = intercept[IllegalArgumentException] {
      Loader.load(ConfigFactory.parseString("""
        |openapi = "s.yaml"
        |auth { type = none }
        |pagination { style = offset, offset-param = "offset", results-path = "/results" }
        |tables { pokemon {
        |  endpoint = "/pokemon"
        |  partition { type = "enum", param = "offset", values = ["0", "100"] }
        |} }
        |""".stripMargin))
    }
    assert(e.getMessage.contains("offset"), e.getMessage)
    assert(e.getMessage.contains("partition.param"), e.getMessage)
  }

  test("date-range partitioning on a pagination parameter is rejected") {
    val e = intercept[IllegalArgumentException] {
      Loader.load(ConfigFactory.parseString("""
        |openapi = "s.yaml"
        |auth { type = none }
        |pagination { style = cursor, cursor-param = "from" }
        |tables { events {
        |  endpoint = "/events"
        |  partition { type = "date-range", column = "at", range = "1d"
        |              start-param = "from", end-param = "to", format = "yyyy-MM-dd" }
        |} }
        |""".stripMargin))
    }
    assert(e.getMessage.contains("cursor-param"), e.getMessage)
  }

  test("date-range end-param colliding with a pagination parameter is rejected") {
    // Covered separately from start-param: with only the start case, deleting the
    // end-param branch of the check leaves every test passing.
    val e = intercept[IllegalArgumentException] {
      Loader.load(ConfigFactory.parseString("""
        |openapi = "s.yaml"
        |auth { type = none }
        |pagination { style = offset, offset-param = "until" }
        |tables { events {
        |  endpoint = "/events"
        |  partition { type = "date-range", column = "at", range = "1d"
        |              start-param = "from", end-param = "until", format = "yyyy-MM-dd" }
        |} }
        |""".stripMargin))
    }
    assert(e.getMessage.contains("partition.end-param"), e.getMessage)
    assert(e.getMessage.contains("until"), e.getMessage)
  }

  test("a per-table pagination override is what gets checked") {
    // Per-table pagination (#217) overrides the source-level block, so the collision has
    // to be judged against the pagination that table actually uses.
    val e = intercept[IllegalArgumentException] {
      Loader.load(ConfigFactory.parseString("""
        |openapi = "s.yaml"
        |auth { type = none }
        |pagination { style = offset, offset-param = "skip" }
        |tables { t {
        |  endpoint = "/t"
        |  pagination { style = offset, offset-param = "start" }
        |  partition { type = "enum", param = "start", values = ["0", "50"] }
        |} }
        |""".stripMargin))
    }
    assert(e.getMessage.contains("start"), e.getMessage)
  }

  test("partitioning on a parameter pagination does not control is allowed") {
    // Guards against the check being so broad it rejects the normal case.
    val cfg = Loader.load(ConfigFactory.parseString("""
      |openapi = "s.yaml"
      |auth { type = none }
      |pagination { style = offset, offset-param = "offset", page-size-param = "limit" }
      |tables { pokemon {
      |  endpoint = "/pokemon"
      |  partition { type = "enum", param = "type", values = ["fire", "water"] }
      |} }
      |""".stripMargin))

    assert(cfg.tables("pokemon").partition.isDefined)
  }

  // --- Offset partitioning ---

  test("offset partitioning parses into windows") {
    val cfg = Loader.load(ConfigFactory.parseString("""
      |openapi = "s.yaml"
      |auth { type = none }
      |pagination { style = offset, offset-param = "offset", results-path = "/results" }
      |tables { t {
      |  endpoint = "/t"
      |  partition { type = "offset", size = 100, count = 4 }
      |} }
      |""".stripMargin))

    assertEquals(cfg.tables("t").partition, Some(PartitionConfig.Offset(size = 100, count = 4)))
  }

  test("offset partitioning is not treated as a pagination collision") {
    // It drives the pagination parameter deliberately, and bounds each window — which is
    // precisely what enum and date-range partitioning cannot do.
    val cfg = Loader.load(ConfigFactory.parseString("""
      |openapi = "s.yaml"
      |auth { type = none }
      |pagination { style = offset, offset-param = "offset" }
      |tables { t { endpoint = "/t", partition { type = "offset", size = 50, count = 2 } } }
      |""".stripMargin))

    assert(cfg.tables("t").partition.isDefined)
  }

  test("offset partitioning requires both size and count") {
    for ((hocon, missing) <- List(
           ("""partition { type = "offset", count = 4 }""", "size"),
           ("""partition { type = "offset", size = 100 }""", "count"))) {
      val e = intercept[IllegalArgumentException] {
        Loader.load(ConfigFactory.parseString(s"""
          |openapi = "s.yaml"
          |auth { type = none }
          |pagination { style = offset }
          |tables { t { endpoint = "/t", $hocon } }
          |""".stripMargin))
      }
      assert(e.getMessage.contains(missing), s"expected '$missing' in: ${e.getMessage}")
    }
  }

  test("offset partition sizes below 1 are rejected") {
    // A zero window would issue a request per partition and read nothing from any of them.
    for (bad <- List("""size = 0, count = 4""", """size = 100, count = 0""")) {
      val e = intercept[IllegalArgumentException] {
        Loader.load(ConfigFactory.parseString(s"""
          |openapi = "s.yaml"
          |auth { type = none }
          |pagination { style = offset }
          |tables { t { endpoint = "/t", partition { type = "offset", $bad } } }
          |""".stripMargin))
      }
      assert(e.getMessage.contains(">= 1"), e.getMessage)
    }
  }

  test("offset partitioning is rejected unless pagination reads the offset") {
    // Cursor and link-header pagination take the next page from the response and
    // `none` fetches one page, so the start offset each partition sends is ignored
    // and all of them read the same records.
    for (style <- List("cursor", "link_header", "none")) {
      val e = intercept[IllegalArgumentException] {
        Loader.load(ConfigFactory.parseString(s"""
          |openapi = "s.yaml"
          |auth { type = none }
          |pagination { style = $style, cursor-param = "c", cursor-path = "/next" }
          |tables { t { endpoint = "/t", partition { type = "offset", size = 50, count = 2 } } }
          |""".stripMargin))
      }
      assert(e.getMessage.contains("pagination style"), s"style=$style: ${e.getMessage}")
    }
  }

  test("offset partitioning is rejected on a streaming response format") {
    // Covered separately from the style check: streaming formats bypass pagination
    // entirely, so `style = offset` passes the first check and the offset is still
    // never sent. Deleting the format branch leaves the style tests passing.
    val e = intercept[IllegalArgumentException] {
      Loader.load(ConfigFactory.parseString("""
        |openapi = "s.yaml"
        |auth { type = none }
        |http { response-format = "ndjson" }
        |pagination { style = offset, offset-param = "offset" }
        |tables { t { endpoint = "/t", partition { type = "offset", size = 50, count = 2 } } }
        |""".stripMargin))
    }
    assert(e.getMessage.contains("without pagination"), e.getMessage)
  }

  test("offset partition range beyond Int.MaxValue is rejected") {
    // `i * size` is Int arithmetic when planning partitions: size 1073741824 over three
    // partitions puts the third start at -2147483648, and the paginator honours it.
    val e = intercept[IllegalArgumentException] {
      Loader.load(ConfigFactory.parseString("""
        |openapi = "s.yaml"
        |auth { type = none }
        |pagination { style = offset }
        |tables { t { endpoint = "/t", partition { type = "offset", size = 1073741824, count = 3 } } }
        |""".stripMargin))
    }
    assert(e.getMessage.contains("Int"), e.getMessage)
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

  test("a date-range partition's range must be at least a millisecond") {
    // A range that rounds to zero milliseconds never advanced, and planning grew its list of
    // partitions without end (#292).
    def withRange(range: String) = ConfigFactory.parseString(
      s"""
        |openapi = "https://example.com/openapi.json"
        |auth { type = none }
        |tables.events {
        |  endpoint = "/events"
        |  partition { type = "date-range", column = "created_at", range = "$range", start-param = "since", end-param = "until" }
        |}
        |""".stripMargin)

    for (bad <- Seq("0 seconds", "-1 day", "500 microseconds")) {
      val ex = intercept[IllegalArgumentException](Loader.load(withRange(bad)))
      assert(ex.getMessage.contains("at least 1 millisecond"), s"$bad: ${ex.getMessage}")
    }
    Loader.load(withRange("7 days"))
  }

  test("a batch checkpoint and a partition can't be combined") {
    // Partitions shared the one checkpoint file: each resumed from the same saved offset,
    // and their writes raced (#294).
    def table(checkpoint: String) = ConfigFactory.parseString(
      s"""
        |openapi = "https://example.com/openapi.json"
        |auth { type = none }
        |pagination { style = offset }
        |tables.items {
        |  endpoint = "/items"
        |  partition { type = "offset", size = 100, count = 2 }
        |  $checkpoint
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException](
      Loader.load(table("""checkpoint { enabled = true, path = "/tmp/ck", mode = offset }"""))
    )
    assert(ex.getMessage.contains("enables a checkpoint and a partition"), ex.getMessage)

    // A streaming table's timestamp settings, without `enabled`, are still fine.
    Loader.load(table("""checkpoint { mode = timestamp, timestamp-param = "since", timestamp-path = "/at" }"""))
  }

  private def oauth2(keys: String) = ConfigFactory.parseString(
    s"""
      |openapi = "https://example.com/openapi.json"
      |auth {
      |  type = oauth2_client
      |  $keys
      |}
      |""".stripMargin)

  test("oauth2_client with client credentials loads") {
    val auth = Loader.load(oauth2(
      """client-id = "id", client-secret = "s", token-url = "https://auth.example.com/token""""
    )).auth
    assertEquals(auth.tokenUrl, Some("https://auth.example.com/token"))
  }

  test("oauth2_client with only a pre-fetched token loads") {
    assertEquals(Loader.load(oauth2("""token = "t"""")).auth.token, Some("t"))
  }

  test("oauth2_client missing part of the client credentials fails at load, naming it") {
    // Before #276 this loaded, then every executor task failed on its first request.
    val ex = intercept[IllegalArgumentException](
      Loader.load(oauth2("""client-id = "id", client-secret = "s""""))
    )
    assert(ex.getMessage.contains("missing token-url"), ex.getMessage)
  }

  private def linkHeaderConfig(sourceStyle: String, tablePagination: String) = ConfigFactory.parseString(
    s"""
      |openapi = "https://example.com/openapi.json"
      |auth { type = none }
      |pagination { style = $sourceStyle, cursor-path = "/next" }
      |tables {
      |  events {
      |    endpoint = "/events"
      |    $tablePagination
      |    checkpoint { enabled = true, path = "/tmp/cp", mode = cursor }
      |  }
      |}
      |""".stripMargin)

  test("a table's own link-header pagination rejects a cursor checkpoint (#317)") {
    val ex = intercept[IllegalArgumentException](
      Loader.load(linkHeaderConfig("cursor", "pagination { style = link_header }"))
    )
    assert(ex.getMessage.contains("link-header pagination"), ex.getMessage)
  }

  test("a table overriding link-header pagination with cursor keeps its cursor checkpoint (#317)") {
    val sc = Loader.load(linkHeaderConfig("link_header", """pagination { style = cursor, cursor-path = "/next" }"""))
    assertEquals(sc.tables("events").pagination.map(_.style), Some(PaginationStyle.Cursor))
  }

  /** A source with one table, `items`, holding `table`'s keys. */
  private def withTable(pagination: String, table: String) = ConfigFactory.parseString(
    s"""
      |openapi = "https://example.com/openapi.json"
      |auth { type = none }
      |pagination { $pagination }
      |tables { items { endpoint = "/items", $table } }
      |""".stripMargin)

  /** The warnings loading `config` logs: what a user loading it is told. */
  private def warningsLoading(config: com.typesafe.config.Config): List[String] = Loader.loadWithWarnings(config)._2

  private val timestampCheckpoint =
    """checkpoint { enabled = true, path = "/tmp/cp", mode = timestamp, timestamp-param = "since", timestamp-path = "/updated_at" }"""

  test("a filter on the timestamp checkpoint's parameter loads, with a warning (#312)") {
    // The docs' setup. Since #310 the filter stays with Spark, so it's safe.
    val warnings = warningsLoading(withTable(
      "style = none",
      s"""filters = [ { param = "since", column = "updated_at", operators = ["gte"] } ], $timestampCheckpoint"""
    ))
    assertEquals(warnings.size, 1)
    assert(warnings.head.contains("'since'") && warnings.head.contains("won't be sent"), warnings.head)
  }

  test("a filter on a parameter pagination sends loads, with a warning, defaults included (#335)") {
    // Offset pagination sends `limit` unless told otherwise. Rejecting this refused configs
    // that load on 0.8.0; the filter is kept for Spark instead.
    val warnings = warningsLoading(withTable(
      "style = offset",
      """filters = [ { param = "limit", column = "size", operators = ["eq"] } ]"""
    ))
    assertEquals(warnings.size, 1)
    assert(warnings.head.contains("'limit'") && warnings.head.contains("page-size-param") &&
      warnings.head.contains("won't be sent"), warnings.head)
  }

  test("0.8.0's PokeAPI example, with its filter on pagination's limit, still loads (#335)") {
    val example = ConfigFactory.parseString(
      """
        |openapi = "https://example.com/openapi.json"
        |auth { type = none }
        |pagination { style = offset, offset-param = "offset", page-size-param = "limit", max-page-size = 100, results-path = "/results" }
        |tables { pokemon { endpoint = "/api/v2/pokemon", data-path = "/results",
        |  filters = [ { param = "limit", column = "limit", operators = ["eq"] } ] } }
        |""".stripMargin)
    assertEquals(warningsLoading(example).count(_.contains("'limit'")), 1)
  }

  test("batch-param is reserved only for a batch join (#335)") {
    // A nested-loop join never sends batch-param, so a filter on that name is a plain filter.
    val warnings = warningsLoading(withTable(
      "style = none",
      """parent-table = "parents", parent-key = "id", join-strategy = "nested_loop", batch-param = "ids",
        |filters = [ { param = "ids", column = "owner", operators = ["eq"] } ]""".stripMargin
    ))
    assertEquals(warnings.filter(_.contains("'ids'")), Nil)
  }

  test("a filter on a batch join's batch-param loads, with a warning (#335)") {
    val warnings = warningsLoading(withTable(
      "style = none",
      """parent-table = "parents", parent-key = "id", join-strategy = "batch", batch-param = "ids",
        |filters = [ { param = "ids", column = "owner", operators = ["eq"] } ]""".stripMargin
    ))
    assertEquals(warnings.size, 1)
    assert(warnings.head.contains("batch-param") && warnings.head.contains("won't be sent"), warnings.head)
  }

  test("a timestamp checkpoint on a parameter pagination sends fails at load (#312)") {
    val ex = intercept[IllegalArgumentException](Loader.load(withTable(
      """style = cursor, cursor-path = "/next", cursor-param = "since"""",
      timestampCheckpoint
    )))
    assert(ex.getMessage.contains("checkpoint.timestamp-param") && ex.getMessage.contains("cursor-param"), ex.getMessage)
  }

  test("a filter on a parameter the table's pagination doesn't send loads (#312)") {
    // Cursor pagination sends no `offset`, so a filter on it collides with nothing.
    val sc = Loader.load(withTable(
      """style = cursor, cursor-path = "/next"""",
      """filters = [ { param = "offset", column = "o", operators = ["eq"] } ]"""
    ))
    assertEquals(Loader.checkParameterCollisions(sc), Nil)
  }
}

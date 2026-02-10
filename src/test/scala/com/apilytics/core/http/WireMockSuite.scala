package com.apilytics.core.http

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.config._
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.circe.parser._
import munit.FunSuite
import org.http4s.Uri

import scala.concurrent.duration._

class WireMockSuite extends FunSuite {

  private var server: WireMockServer = _

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
  }

  override def afterEach(context: AfterEach): Unit = {
    server.stop()
  }

  private def baseUri: Uri = Uri.unsafeFromString(s"http://localhost:${server.port()}")

  private val defaultHttp = HttpConfig(maxRetries = 2, maxBackoff = 1.second, timeout = 5.seconds)
  // Use Accept header as a no-op for unauthenticated requests
  private val noAuth = AuthConfig(
    authType = AuthType.Header,
    headerName = Some("Accept"),
    headerValue = Some("application/json")
  )

  // ============== PAGINATION TESTS ==============

  test("cursor pagination follows cursor until empty") {
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("limit", equalTo("10"))
        .willReturn(okJson("""{"items": [{"id": 1}], "next_cursor": "abc123"}"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("cursor", equalTo("abc123"))
        .withQueryParam("limit", equalTo("10"))
        .willReturn(okJson("""{"items": [{"id": 2}], "next_cursor": ""}"""))
    )

    val pagination = PaginationConfig(
      style = PaginationStyle.Cursor,
      cursorPath = Some("/next_cursor"),
      cursorParam = Some("cursor"),
      pageSizeParam = Some("limit"),
      maxPageSize = 10
    )

    val pages = Client.resource(defaultHttp, noAuth).use { client =>
      Paginator.pages(client, baseUri.addPath("items"), Map.empty, pagination)
        .compile.toList
    }.unsafeRunSync()

    assertEquals(pages.size, 2)
  }

  test("offset pagination increments offset") {
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("offset", equalTo("0"))
        .withQueryParam("limit", equalTo("5"))
        .willReturn(okJson("""[{"id": 1}, {"id": 2}]"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("offset", equalTo("5"))
        .withQueryParam("limit", equalTo("5"))
        .willReturn(okJson("""[{"id": 3}]"""))
    )

    val pagination = PaginationConfig(
      style = PaginationStyle.Offset,
      offsetParam = Some("offset"),
      pageSizeParam = Some("limit"),
      maxPageSize = 5
    )

    val pages = Client.resource(defaultHttp, noAuth).use { client =>
      Paginator.pages(client, baseUri.addPath("items"), Map.empty, pagination, limit = Some(7))
        .compile.toList
    }.unsafeRunSync()

    assertEquals(pages.size, 2)
  }

  test("offset pagination stops on empty top-level array") {
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("offset", equalTo("0"))
        .withQueryParam("limit", equalTo("2"))
        .willReturn(okJson("""[{"id": 1}, {"id": 2}]"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("offset", equalTo("2"))
        .withQueryParam("limit", equalTo("2"))
        .willReturn(okJson("""[{"id": 3}]"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("offset", equalTo("4"))
        .withQueryParam("limit", equalTo("2"))
        .willReturn(okJson("""[]"""))
    )

    val pagination = PaginationConfig(
      style = PaginationStyle.Offset,
      offsetParam = Some("offset"),
      pageSizeParam = Some("limit"),
      maxPageSize = 2
    )

    val pages = Client.resource(defaultHttp, noAuth).use { client =>
      Paginator.pages(client, baseUri.addPath("items"), Map.empty, pagination)
        .compile.toList
    }.unsafeRunSync()

    assertEquals(pages.size, 2)
    // Verify 3 requests were made (page 1, page 2, empty page 3 that stopped pagination)
    assertEquals(server.getAllServeEvents.size(), 3)
  }

  test("offset pagination stops on empty results-path array") {
    server.stubFor(
      get(urlPathEqualTo("/api"))
        .withQueryParam("offset", equalTo("0"))
        .withQueryParam("limit", equalTo("2"))
        .willReturn(okJson("""{"count": 3, "results": [{"name": "a"}, {"name": "b"}]}"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/api"))
        .withQueryParam("offset", equalTo("2"))
        .withQueryParam("limit", equalTo("2"))
        .willReturn(okJson("""{"count": 3, "results": [{"name": "c"}]}"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/api"))
        .withQueryParam("offset", equalTo("4"))
        .withQueryParam("limit", equalTo("2"))
        .willReturn(okJson("""{"count": 3, "results": []}"""))
    )

    val pagination = PaginationConfig(
      style = PaginationStyle.Offset,
      offsetParam = Some("offset"),
      pageSizeParam = Some("limit"),
      maxPageSize = 2,
      resultsPath = Some("/results")
    )

    val pages = Client.resource(defaultHttp, noAuth).use { client =>
      Paginator.pages(client, baseUri.addPath("api"), Map.empty, pagination)
        .compile.toList
    }.unsafeRunSync()

    assertEquals(pages.size, 2)
    assertEquals(server.getAllServeEvents.size(), 3)
  }

  test("offset pagination respects max-pages safety limit") {
    // Stub that always returns data (would loop forever without max-pages)
    server.stubFor(
      get(urlPathEqualTo("/infinite"))
        .willReturn(okJson("""{"data": [{"id": 1}]}"""))
    )

    val pagination = PaginationConfig(
      style = PaginationStyle.Offset,
      offsetParam = Some("offset"),
      pageSizeParam = Some("limit"),
      maxPageSize = 10,
      maxPages = 3
    )

    val pages = Client.resource(defaultHttp, noAuth).use { client =>
      Paginator.pages(client, baseUri.addPath("infinite"), Map.empty, pagination)
        .compile.toList
    }.unsafeRunSync()

    assertEquals(pages.size, 3)
  }

  test("link header pagination follows next link") {
    val page2Url = s"http://localhost:${server.port()}/items?page=2"
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("per_page", equalTo("5"))
        .willReturn(
          okJson("""[{"id": 1}]""")
            .withHeader("Link", s"""<$page2Url>; rel="next"""")
        )
    )
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("page", equalTo("2"))
        .willReturn(okJson("""[{"id": 2}]"""))
    )

    val pagination = PaginationConfig(
      style = PaginationStyle.LinkHeader,
      pageSizeParam = Some("per_page"),
      maxPageSize = 5
    )

    val pages = Client.resource(defaultHttp, noAuth).use { client =>
      Paginator.pages(client, baseUri.addPath("items"), Map.empty, pagination)
        .compile.toList
    }.unsafeRunSync()

    assertEquals(pages.size, 2)
  }

  test("no pagination returns single page") {
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .willReturn(okJson("""[{"id": 1}, {"id": 2}]"""))
    )

    val pagination = PaginationConfig(style = PaginationStyle.None, maxPageSize = 100)

    val pages = Client.resource(defaultHttp, noAuth).use { client =>
      Paginator.pages(client, baseUri.addPath("items"), Map.empty, pagination)
        .compile.toList
    }.unsafeRunSync()

    assertEquals(pages.size, 1)
  }

  // ============== AUTH TESTS ==============

  test("bearer auth adds Authorization header") {
    server.stubFor(
      get(urlPathEqualTo("/secure"))
        .withHeader("Authorization", equalTo("Bearer secret-token"))
        .willReturn(okJson("""{"status": "ok"}"""))
    )

    val auth = AuthConfig(authType = AuthType.Bearer, token = Some("secret-token"))

    val resp = Client.resource(defaultHttp, auth).use { client =>
      client.get(baseUri.addPath("secure"))
    }.unsafeRunSync()

    assertEquals(resp.status, 200)
  }

  test("basic auth adds base64 encoded header") {
    // user:pass base64 = dXNlcjpwYXNz
    server.stubFor(
      get(urlPathEqualTo("/secure"))
        .withHeader("Authorization", equalTo("Basic dXNlcjpwYXNz"))
        .willReturn(okJson("""{"status": "ok"}"""))
    )

    val auth = AuthConfig(authType = AuthType.Basic, username = Some("user"), password = Some("pass"))

    val resp = Client.resource(defaultHttp, auth).use { client =>
      client.get(baseUri.addPath("secure"))
    }.unsafeRunSync()

    assertEquals(resp.status, 200)
  }

  test("custom header auth adds specified header") {
    server.stubFor(
      get(urlPathEqualTo("/secure"))
        .withHeader("X-API-Key", equalTo("my-api-key"))
        .willReturn(okJson("""{"status": "ok"}"""))
    )

    val auth = AuthConfig(
      authType = AuthType.Header,
      headerName = Some("X-API-Key"),
      headerValue = Some("my-api-key")
    )

    val resp = Client.resource(defaultHttp, auth).use { client =>
      client.get(baseUri.addPath("secure"))
    }.unsafeRunSync()

    assertEquals(resp.status, 200)
  }

  test("auth failure returns error") {
    server.stubFor(
      get(urlPathEqualTo("/secure"))
        .willReturn(unauthorized().withBody("Invalid token"))
    )

    val auth = AuthConfig(authType = AuthType.Bearer, token = Some("bad-token"))

    val error = intercept[RuntimeException] {
      Client.resource(defaultHttp, auth).use { client =>
        client.get(baseUri.addPath("secure"))
      }.unsafeRunSync()
    }

    assert(error.getMessage.contains("401"))
  }

  // ============== RETRY TESTS ==============

  test("retries on 429 with exponential backoff") {
    server.stubFor(
      get(urlPathEqualTo("/rate-limited"))
        .inScenario("rate-limit")
        .whenScenarioStateIs("Started")
        .willReturn(aResponse().withStatus(429))
        .willSetStateTo("retry1")
    )
    server.stubFor(
      get(urlPathEqualTo("/rate-limited"))
        .inScenario("rate-limit")
        .whenScenarioStateIs("retry1")
        .willReturn(okJson("""{"status": "ok"}"""))
    )

    val resp = Client.resource(defaultHttp, noAuth).use { client =>
      client.get(baseUri.addPath("rate-limited"))
    }.unsafeRunSync()

    assertEquals(resp.status, 200)
    assertEquals(server.getAllServeEvents.size(), 2)
  }

  test("retries on 5xx errors") {
    server.stubFor(
      get(urlPathEqualTo("/unstable"))
        .inScenario("server-error")
        .whenScenarioStateIs("Started")
        .willReturn(serverError().withBody("Internal error"))
        .willSetStateTo("recovered")
    )
    server.stubFor(
      get(urlPathEqualTo("/unstable"))
        .inScenario("server-error")
        .whenScenarioStateIs("recovered")
        .willReturn(okJson("""{"status": "recovered"}"""))
    )

    val resp = Client.resource(defaultHttp, noAuth).use { client =>
      client.get(baseUri.addPath("unstable"))
    }.unsafeRunSync()

    assertEquals(resp.status, 200)
  }

  test("respects Retry-After header with seconds") {
    server.stubFor(
      get(urlPathEqualTo("/rate-limited"))
        .inScenario("retry-after")
        .whenScenarioStateIs("Started")
        .willReturn(
          aResponse()
            .withStatus(429)
            .withHeader("Retry-After", "1")
        )
        .willSetStateTo("ready")
    )
    server.stubFor(
      get(urlPathEqualTo("/rate-limited"))
        .inScenario("retry-after")
        .whenScenarioStateIs("ready")
        .willReturn(okJson("""{"status": "ok"}"""))
    )

    val start = System.currentTimeMillis()
    val resp = Client.resource(defaultHttp, noAuth).use { client =>
      client.get(baseUri.addPath("rate-limited"))
    }.unsafeRunSync()
    val elapsed = System.currentTimeMillis() - start

    assertEquals(resp.status, 200)
    assert(elapsed >= 900, s"Expected at least 1s delay, got ${elapsed}ms")
  }

  test("gives up after max retries") {
    server.stubFor(
      get(urlPathEqualTo("/always-fails"))
        .willReturn(serverError().withBody("Always fails"))
    )

    val httpWithFewRetries = HttpConfig(maxRetries = 1, maxBackoff = 100.millis, timeout = 5.seconds)

    val error = intercept[RuntimeException] {
      Client.resource(httpWithFewRetries, noAuth).use { client =>
        client.get(baseUri.addPath("always-fails"))
      }.unsafeRunSync()
    }

    assert(error.getMessage.contains("500"))
    assertEquals(server.getAllServeEvents.size(), 2) // 1 initial + 1 retry
  }

  // ============== JSON HANDLING TESTS ==============

  test("handles valid JSON response") {
    server.stubFor(
      get(urlPathEqualTo("/data"))
        .willReturn(okJson("""{"name": "test", "value": 42}"""))
    )

    val resp = Client.resource(defaultHttp, noAuth).use { client =>
      client.get(baseUri.addPath("data"))
    }.unsafeRunSync()

    assertEquals(resp.json.hcursor.get[String]("name").toOption, Some("test"))
    assertEquals(resp.json.hcursor.get[Int]("value").toOption, Some(42))
  }

  test("Converter handles missing fields as null") {
    val json = parse("""[{"id": 1, "name": "test"}, {"id": 2}]""").getOrElse(throw new Exception("parse failed"))
    val records = Converter.extractRecords(json, None)

    assertEquals(records.size, 2)
    // Second record is missing "name" field
    val secondRecord = records(1)
    assert(secondRecord.hcursor.get[String]("name").isLeft)
  }

  test("extracts nested data via dataPath") {
    val json = parse("""{"data": {"items": [{"id": 1}, {"id": 2}]}, "meta": {}}""").getOrElse(throw new Exception("parse failed"))
    val records = Converter.extractRecords(json, Some("/data/items"))

    assertEquals(records.size, 2)
  }

  // ============== QUERY PARAMS TESTS ==============

  test("query params are passed to server") {
    server.stubFor(
      get(urlPathEqualTo("/search"))
        .withQueryParam("q", equalTo("test"))
        .withQueryParam("limit", equalTo("10"))
        .willReturn(okJson("""{"results": []}"""))
    )

    val resp = Client.resource(defaultHttp, noAuth).use { client =>
      client.get(baseUri.addPath("search"), Map("q" -> "test", "limit" -> "10"))
    }.unsafeRunSync()

    assertEquals(resp.status, 200)
  }

  // ============== RESPONSE HEADERS TESTS ==============

  test("response headers are captured") {
    server.stubFor(
      get(urlPathEqualTo("/headers"))
        .willReturn(
          okJson("""{}""")
            .withHeader("X-Request-Id", "abc123")
            .withHeader("X-Rate-Limit-Remaining", "99")
        )
    )

    val resp = Client.resource(defaultHttp, noAuth).use { client =>
      client.get(baseUri.addPath("headers"))
    }.unsafeRunSync()

    assertEquals(resp.headers.get("X-Request-Id"), Some("abc123"))
    assertEquals(resp.headers.get("X-Rate-Limit-Remaining"), Some("99"))
  }

  // ============== OAUTH2 CLIENT CREDENTIALS TESTS ==============

  test("OAuth2 fetches token and uses it for API requests") {
    // Token endpoint
    server.stubFor(
      post(urlPathEqualTo("/oauth/token"))
        .withRequestBody(containing("grant_type=client_credentials"))
        .withRequestBody(containing("client_id=test-client"))
        .withRequestBody(containing("client_secret=test-secret"))
        .willReturn(okJson("""{"access_token": "fresh-token", "expires_in": 3600}"""))
    )
    // API endpoint requiring auth
    server.stubFor(
      get(urlPathEqualTo("/api/data"))
        .withHeader("Authorization", equalTo("Bearer fresh-token"))
        .willReturn(okJson("""{"status": "ok"}"""))
    )

    val authConfig = AuthConfig(
      authType = AuthType.OAuth2Client,
      clientId = Some("test-client"),
      clientSecret = Some("test-secret"),
      tokenUrl = Some(s"http://localhost:${server.port()}/oauth/token")
    )

    val resp = Client.resourceWithOAuth2(defaultHttp, authConfig).use { client =>
      client.get(baseUri.addPath("api/data"))
    }.unsafeRunSync()

    assertEquals(resp.status, 200)
    // Verify token was fetched
    server.verify(1, postRequestedFor(urlPathEqualTo("/oauth/token")))
  }

  test("OAuth2 refreshes token on 401 and retries") {
    // Use scenarios to return different tokens on each POST
    server.stubFor(
      post(urlPathEqualTo("/oauth/token"))
        .inScenario("oauth-refresh")
        .whenScenarioStateIs("Started")
        .willReturn(okJson("""{"access_token": "first-token", "expires_in": 3600}"""))
        .willSetStateTo("first-token-issued")
    )
    server.stubFor(
      post(urlPathEqualTo("/oauth/token"))
        .inScenario("oauth-refresh")
        .whenScenarioStateIs("first-token-issued")
        .willReturn(okJson("""{"access_token": "refreshed-token", "expires_in": 3600}"""))
    )
    // First API call with first token returns 401
    server.stubFor(
      get(urlPathEqualTo("/api/data"))
        .withHeader("Authorization", equalTo("Bearer first-token"))
        .willReturn(unauthorized().withBody("Token expired"))
    )
    // Retry with refreshed token succeeds
    server.stubFor(
      get(urlPathEqualTo("/api/data"))
        .withHeader("Authorization", equalTo("Bearer refreshed-token"))
        .willReturn(okJson("""{"status": "ok"}"""))
    )

    val authConfig = AuthConfig(
      authType = AuthType.OAuth2Client,
      clientId = Some("test-client"),
      clientSecret = Some("test-secret"),
      tokenUrl = Some(s"http://localhost:${server.port()}/oauth/token")
    )

    val resp = Client.resourceWithOAuth2(defaultHttp, authConfig).use { client =>
      client.get(baseUri.addPath("api/data"))
    }.unsafeRunSync()

    assertEquals(resp.status, 200)
    // Verify: initial token fetch + refresh
    server.verify(2, postRequestedFor(urlPathEqualTo("/oauth/token")))
    // Verify: 2 API calls (first 401, second 200)
    server.verify(2, getRequestedFor(urlPathEqualTo("/api/data")))
  }

  test("OAuth2 fails after refresh if still 401") {
    // Token endpoint always returns same token
    server.stubFor(
      post(urlPathEqualTo("/oauth/token"))
        .willReturn(okJson("""{"access_token": "bad-token", "expires_in": 3600}"""))
    )
    // API always returns 401
    server.stubFor(
      get(urlPathEqualTo("/api/data"))
        .willReturn(unauthorized().withBody("Invalid token"))
    )

    val authConfig = AuthConfig(
      authType = AuthType.OAuth2Client,
      clientId = Some("test-client"),
      clientSecret = Some("test-secret"),
      tokenUrl = Some(s"http://localhost:${server.port()}/oauth/token")
    )

    val error = intercept[RuntimeException] {
      Client.resourceWithOAuth2(defaultHttp, authConfig).use { client =>
        client.get(baseUri.addPath("api/data"))
      }.unsafeRunSync()
    }

    assert(error.getMessage.contains("401"))
    // Initial fetch + refresh = 2 token fetches
    server.verify(2, postRequestedFor(urlPathEqualTo("/oauth/token")))
    // 2 API calls (both 401)
    server.verify(2, getRequestedFor(urlPathEqualTo("/api/data")))
  }

  test("OAuth2 token fetch failure throws") {
    server.stubFor(
      post(urlPathEqualTo("/oauth/token"))
        .willReturn(unauthorized().withBody("Invalid credentials"))
    )

    val authConfig = AuthConfig(
      authType = AuthType.OAuth2Client,
      clientId = Some("bad-client"),
      clientSecret = Some("bad-secret"),
      tokenUrl = Some(s"http://localhost:${server.port()}/oauth/token")
    )

    val error = intercept[ApiError] {
      Client.resourceWithOAuth2(defaultHttp, authConfig).use { client =>
        client.get(baseUri.addPath("api/data"))
      }.unsafeRunSync()
    }

    assert(error.statusCode == 401)
    assert(error.message == "Authentication failed")
  }

  test("OAuth2 caches token across requests") {
    server.stubFor(
      post(urlPathEqualTo("/oauth/token"))
        .willReturn(okJson("""{"access_token": "cached-token", "expires_in": 3600}"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/api/data"))
        .withHeader("Authorization", equalTo("Bearer cached-token"))
        .willReturn(okJson("""{"id": 1}"""))
    )

    val authConfig = AuthConfig(
      authType = AuthType.OAuth2Client,
      clientId = Some("test-client"),
      clientSecret = Some("test-secret"),
      tokenUrl = Some(s"http://localhost:${server.port()}/oauth/token")
    )

    Client.resourceWithOAuth2(defaultHttp, authConfig).use { client =>
      for {
        _ <- client.get(baseUri.addPath("api/data"))
        _ <- client.get(baseUri.addPath("api/data"))
        _ <- client.get(baseUri.addPath("api/data"))
      } yield ()
    }.unsafeRunSync()

    // Token should only be fetched once
    server.verify(1, postRequestedFor(urlPathEqualTo("/oauth/token")))
    // All 3 API requests should succeed
    server.verify(3, getRequestedFor(urlPathEqualTo("/api/data")))
  }

  test("OAuth2 proactively refreshes expired token during pagination") {
    // Issue #79: Token expires mid-scan, should refresh before 401
    // Token expires after 2 seconds, but we'll expire it 60s early (so effectively 0s)
    server.stubFor(
      post(urlPathEqualTo("/oauth/token"))
        .inScenario("pagination-refresh")
        .whenScenarioStateIs("Started")
        .willReturn(okJson("""{"access_token": "token-1", "expires_in": 1}"""))
        .willSetStateTo("first-token-issued")
    )
    server.stubFor(
      post(urlPathEqualTo("/oauth/token"))
        .inScenario("pagination-refresh")
        .whenScenarioStateIs("first-token-issued")
        .willReturn(okJson("""{"access_token": "token-2", "expires_in": 3600}"""))
    )

    // Page 1 - uses token-1
    val page2Url = s"http://localhost:${server.port()}/items?page=2"
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withHeader("Authorization", equalTo("Bearer token-1"))
        .withQueryParam("per_page", equalTo("5"))
        .willReturn(
          okJson("""[{"id": 1}]""")
            .withHeader("Link", s"""<$page2Url>; rel="next"""")
        )
    )

    // Page 2 - token-1 expired, should use token-2 after proactive refresh
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withHeader("Authorization", equalTo("Bearer token-2"))
        .withQueryParam("page", equalTo("2"))
        .willReturn(okJson("""[{"id": 2}]"""))
    )

    val authConfig = AuthConfig(
      authType = AuthType.OAuth2Client,
      clientId = Some("test-client"),
      clientSecret = Some("test-secret"),
      tokenUrl = Some(s"http://localhost:${server.port()}/oauth/token")
    )

    val pagination = PaginationConfig(
      style = PaginationStyle.LinkHeader,
      pageSizeParam = Some("per_page"),
      maxPageSize = 5
    )

    // Wait for token-1 to expire (it has 1s expiry, minus 60s buffer = already expired)
    // The first page fetch gets token-1, then we sleep to ensure expiry
    val pages = Client.resourceWithOAuth2(defaultHttp, authConfig).use { client =>
      Paginator.pages(client, baseUri.addPath("items"), Map.empty, pagination)
        .evalTap(_ => IO.sleep(100.millis)) // Small delay between pages
        .compile.toList
    }.unsafeRunSync()

    assertEquals(pages.size, 2)
    // Token should be refreshed proactively before page 2
    server.verify(2, postRequestedFor(urlPathEqualTo("/oauth/token")))
  }

  // ============== DATE-RANGE PARTITIONING TESTS ==============

  test("date-range partitioning sends correct bounded params to API") {
    // Simulate an API that accepts start/end date params
    server.stubFor(
      get(urlPathEqualTo("/events"))
        .withQueryParam("created_after", equalTo("2024-01-01T00:00:00Z"))
        .withQueryParam("created_before", equalTo("2024-01-02T00:00:00Z"))
        .willReturn(okJson("""[{"id": 1, "name": "event1"}]"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/events"))
        .withQueryParam("created_after", equalTo("2024-01-02T00:00:00Z"))
        .withQueryParam("created_before", equalTo("2024-01-03T00:00:00Z"))
        .willReturn(okJson("""[{"id": 2, "name": "event2"}]"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/events"))
        .withQueryParam("created_after", equalTo("2024-01-03T00:00:00Z"))
        .withQueryParam("created_before", equalTo("2024-01-04T00:00:00Z"))
        .willReturn(okJson("""[{"id": 3, "name": "event3"}]"""))
    )

    val pagination = PaginationConfig(style = PaginationStyle.None, maxPageSize = 100)

    // Simulate 3 partitions making requests (like Spark would)
    val partitionParams = List(
      Map("created_after" -> "2024-01-01T00:00:00Z", "created_before" -> "2024-01-02T00:00:00Z"),
      Map("created_after" -> "2024-01-02T00:00:00Z", "created_before" -> "2024-01-03T00:00:00Z"),
      Map("created_after" -> "2024-01-03T00:00:00Z", "created_before" -> "2024-01-04T00:00:00Z")
    )

    val allResults = Client.resource(defaultHttp, noAuth).use { client =>
      import cats.implicits._
      partitionParams.traverse { params =>
        Paginator.pages(client, baseUri.addPath("events"), params, pagination)
          .compile.toList
      }
    }.unsafeRunSync()

    // Each partition should return 1 page with 1 event
    assertEquals(allResults.size, 3)
    allResults.foreach(pages => assertEquals(pages.size, 1))

    // Verify all 3 partition requests were made with correct bounded params
    server.verify(1, getRequestedFor(urlPathEqualTo("/events"))
      .withQueryParam("created_after", equalTo("2024-01-01T00:00:00Z"))
      .withQueryParam("created_before", equalTo("2024-01-02T00:00:00Z")))
    server.verify(1, getRequestedFor(urlPathEqualTo("/events"))
      .withQueryParam("created_after", equalTo("2024-01-02T00:00:00Z"))
      .withQueryParam("created_before", equalTo("2024-01-03T00:00:00Z")))
    server.verify(1, getRequestedFor(urlPathEqualTo("/events"))
      .withQueryParam("created_after", equalTo("2024-01-03T00:00:00Z"))
      .withQueryParam("created_before", equalTo("2024-01-04T00:00:00Z")))
  }

  test("date-range partitioning works with pagination") {
    // Each partition may have multiple pages
    val page2Day1 = s"http://localhost:${server.port()}/events?created_after=2024-01-01T00:00:00Z&created_before=2024-01-02T00:00:00Z&page=2"

    server.stubFor(
      get(urlPathEqualTo("/events"))
        .withQueryParam("created_after", equalTo("2024-01-01T00:00:00Z"))
        .withQueryParam("created_before", equalTo("2024-01-02T00:00:00Z"))
        .withQueryParam("per_page", equalTo("10"))
        .willReturn(
          okJson("""[{"id": 1}]""")
            .withHeader("Link", s"""<$page2Day1>; rel="next"""")
        )
    )
    server.stubFor(
      get(urlPathEqualTo("/events"))
        .withQueryParam("created_after", equalTo("2024-01-01T00:00:00Z"))
        .withQueryParam("created_before", equalTo("2024-01-02T00:00:00Z"))
        .withQueryParam("page", equalTo("2"))
        .willReturn(okJson("""[{"id": 2}]"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/events"))
        .withQueryParam("created_after", equalTo("2024-01-02T00:00:00Z"))
        .withQueryParam("created_before", equalTo("2024-01-03T00:00:00Z"))
        .withQueryParam("per_page", equalTo("10"))
        .willReturn(okJson("""[{"id": 3}]"""))
    )

    val pagination = PaginationConfig(
      style = PaginationStyle.LinkHeader,
      pageSizeParam = Some("per_page"),
      maxPageSize = 10
    )

    // Partition 1: 2 pages, Partition 2: 1 page
    val partitionParams = List(
      Map("created_after" -> "2024-01-01T00:00:00Z", "created_before" -> "2024-01-02T00:00:00Z"),
      Map("created_after" -> "2024-01-02T00:00:00Z", "created_before" -> "2024-01-03T00:00:00Z")
    )

    val allResults = Client.resource(defaultHttp, noAuth).use { client =>
      import cats.implicits._
      partitionParams.traverse { params =>
        Paginator.pages(client, baseUri.addPath("events"), params, pagination)
          .compile.toList
      }
    }.unsafeRunSync()

    // Partition 1 has 2 pages, Partition 2 has 1 page
    assertEquals(allResults(0).size, 2)
    assertEquals(allResults(1).size, 1)
  }

  test("OAuth2 refreshes token on 401 during pagination") {
    // Fallback: if proactive refresh fails, 401 triggers refresh
    server.stubFor(
      post(urlPathEqualTo("/oauth/token"))
        .inScenario("pagination-401")
        .whenScenarioStateIs("Started")
        .willReturn(okJson("""{"access_token": "token-1", "expires_in": 3600}"""))
        .willSetStateTo("first-token-issued")
    )
    server.stubFor(
      post(urlPathEqualTo("/oauth/token"))
        .inScenario("pagination-401")
        .whenScenarioStateIs("first-token-issued")
        .willReturn(okJson("""{"access_token": "token-2", "expires_in": 3600}"""))
    )

    // Page 1 succeeds with token-1
    val page2Url = s"http://localhost:${server.port()}/items?page=2"
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withHeader("Authorization", equalTo("Bearer token-1"))
        .withQueryParam("per_page", equalTo("5"))
        .willReturn(
          okJson("""[{"id": 1}]""")
            .withHeader("Link", s"""<$page2Url>; rel="next"""")
        )
    )

    // Page 2 with token-1 returns 401 (server-side expiry)
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withHeader("Authorization", equalTo("Bearer token-1"))
        .withQueryParam("page", equalTo("2"))
        .willReturn(unauthorized().withBody("Token expired"))
    )

    // Page 2 retry with token-2 succeeds
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withHeader("Authorization", equalTo("Bearer token-2"))
        .withQueryParam("page", equalTo("2"))
        .willReturn(okJson("""[{"id": 2}]"""))
    )

    val authConfig = AuthConfig(
      authType = AuthType.OAuth2Client,
      clientId = Some("test-client"),
      clientSecret = Some("test-secret"),
      tokenUrl = Some(s"http://localhost:${server.port()}/oauth/token")
    )

    val pagination = PaginationConfig(
      style = PaginationStyle.LinkHeader,
      pageSizeParam = Some("per_page"),
      maxPageSize = 5
    )

    val pages = Client.resourceWithOAuth2(defaultHttp, authConfig).use { client =>
      Paginator.pages(client, baseUri.addPath("items"), Map.empty, pagination)
        .compile.toList
    }.unsafeRunSync()

    assertEquals(pages.size, 2)
    // Token refreshed after 401
    server.verify(2, postRequestedFor(urlPathEqualTo("/oauth/token")))
    // Page 2 was fetched twice (401, then success)
    server.verify(2, getRequestedFor(urlPathEqualTo("/items")).withQueryParam("page", equalTo("2")))
  }
}

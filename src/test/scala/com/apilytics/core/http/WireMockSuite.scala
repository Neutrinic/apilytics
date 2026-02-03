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
}

package com.apilytics.core.http

import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig, ResponseCacheConfig}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.http4s.Uri

import scala.concurrent.duration._

/** The response cache is shared per JVM, so its keys have to say whose response it is (#278).
  *
  * Keys used to be the request path and parameters only. Two sources requesting the same
  * path, on different hosts or with different credentials, then shared entries, and one
  * catalog could be served another's data.
  */
class ResponseCacheIsolationSuite extends FunSuite {

  private var a: WireMockServer = _
  private var b: WireMockServer = _

  private val http = HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 5.seconds)
  private val cacheConfig = ResponseCacheConfig(enabled = true, ttl = 1.minute, maxEntries = 100)

  override def beforeEach(context: BeforeEach): Unit = {
    ResponseCache.clearSingletons()
    a = new WireMockServer(wireMockConfig().dynamicPort()); a.start()
    b = new WireMockServer(wireMockConfig().dynamicPort()); b.start()
    a.stubFor(get(urlPathEqualTo("/items")).willReturn(okJson("""{"from": "a"}""")))
    b.stubFor(get(urlPathEqualTo("/items")).willReturn(okJson("""{"from": "b"}""")))
  }

  override def afterEach(context: AfterEach): Unit = {
    a.stop(); b.stop()
    ResponseCache.clearSingletons()
  }

  private def fetch(server: WireMockServer, auth: AuthConfig): String =
    Client.resource(http, auth, ResponseCache.fromConfig(cacheConfig)).use { client =>
      client.get(Uri.unsafeFromString(s"http://localhost:${server.port()}/items"), Map("page" -> "1"))
    }.unsafeRunSync().json.hcursor.get[String]("from").toOption.orNull

  private val none = AuthConfig(authType = AuthType.None)
  private def bearer(token: String) = AuthConfig(authType = AuthType.Bearer, token = Some(token))

  test("the same path on two hosts is cached separately") {
    assertEquals(fetch(a, none), "a")
    assertEquals(fetch(b, none), "b", "host b was served host a's cached response")
  }

  test("the same request with two sets of credentials is cached separately") {
    a.stubFor(get(urlPathEqualTo("/items")).withHeader("Authorization", equalTo("Bearer one"))
      .willReturn(okJson("""{"from": "one"}""")))
    a.stubFor(get(urlPathEqualTo("/items")).withHeader("Authorization", equalTo("Bearer two"))
      .willReturn(okJson("""{"from": "two"}""")))

    assertEquals(fetch(a, bearer("one")), "one")
    assertEquals(fetch(a, bearer("two")), "two", "credentials two were served credentials one's cached response")
  }

  test("link-header pages whose query sits in the URL are cached separately") {
    // Link-header pagination follows each next link with its query inside the URL and an
    // empty parameter map, so a key built from the path and the map alone gave page 2 and
    // page 3 the same entry: page 3 came back as page 2, and the walk repeated it (#286).
    val base = s"http://localhost:${a.port()}"
    a.stubFor(get(urlPathEqualTo("/pages")).withQueryParam("page", absent())
      .willReturn(okJson("""{"page": 1}""").withHeader("Link", s"""<$base/pages?page=2>; rel="next"""")))
    a.stubFor(get(urlPathEqualTo("/pages")).withQueryParam("page", equalTo("2"))
      .willReturn(okJson("""{"page": 2}""").withHeader("Link", s"""<$base/pages?page=3>; rel="next"""")))
    a.stubFor(get(urlPathEqualTo("/pages")).withQueryParam("page", equalTo("3"))
      .willReturn(okJson("""{"page": 3}""")))

    val config = com.apilytics.core.config.PaginationConfig(style = com.apilytics.core.config.PaginationStyle.LinkHeader)
    val pages = Client.resource(http, none, ResponseCache.fromConfig(cacheConfig)).use { client =>
      Paginator.pages(client, Uri.unsafeFromString(s"$base/pages"), Map.empty, config)
        .map(_.hcursor.get[Int]("page").toOption.get).compile.toList
    }.unsafeRunSync()

    assertEquals(pages, List(1, 2, 3))
  }

  test("cache keys are logged without their parameter values") {
    // Keys carry query strings, which can carry tokens; logs keep only the names.
    val key = "https://api.example.com#0a1b/items?api_key=secret&page=2?per_page=100"
    assertEquals(ResponseCache.forLog(key), "https://api.example.com#0a1b/items?api_key=***&page=***?per_page=***")
  }

  test("a repeated request from the same source is still served from the cache") {
    assertEquals(fetch(a, bearer("one")), "a")
    assertEquals(fetch(a, bearer("one")), "a")
    a.verify(1, getRequestedFor(urlPathEqualTo("/items")))
  }
}

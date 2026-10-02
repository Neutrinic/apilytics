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

  test("a repeated request from the same source is still served from the cache") {
    assertEquals(fetch(a, bearer("one")), "a")
    assertEquals(fetch(a, bearer("one")), "a")
    a.verify(1, getRequestedFor(urlPathEqualTo("/items")))
  }
}

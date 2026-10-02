package com.apilytics.core.http

import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig, PaginationConfig, PaginationStyle}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.http4s.Uri

import scala.concurrent.duration._

/** Link-header pagination never carries a source's credentials to another origin (#288).
  *
  * The paginator followed whatever `next` URL a response named, and the client applied the
  * source's authentication to every request, so a `Link` header pointing elsewhere sent the
  * API's token there, including over plain HTTP.
  */
class LinkHeaderOriginSuite extends FunSuite {

  private var api: WireMockServer = _
  private var other: WireMockServer = _

  private val http = HttpConfig(maxRetries = 0, maxBackoff = 0.seconds, timeout = 5.seconds)
  private val linkHeader = PaginationConfig(style = PaginationStyle.LinkHeader)
  private val bearer = AuthConfig(authType = AuthType.Bearer, token = Some("secret-token"))
  private val none = AuthConfig(authType = AuthType.None)

  override def beforeEach(context: BeforeEach): Unit = {
    api = new WireMockServer(wireMockConfig().dynamicPort()); api.start()
    other = new WireMockServer(wireMockConfig().dynamicPort()); other.start()
    other.stubFor(get(anyUrl()).willReturn(okJson("""{"page": 2}""")))
  }

  override def afterEach(context: AfterEach): Unit = { api.stop(); other.stop() }

  private def apiBase = s"http://localhost:${api.port()}"

  private def firstPageLinkingTo(next: String): Unit =
    api.stubFor(get(urlPathEqualTo("/items")).withQueryParam("page", absent())
      .willReturn(okJson("""{"page": 1}""").withHeader("Link", s"""<$next>; rel="next"""")))

  private def walk(auth: AuthConfig) =
    Client.resource(http, auth).use { client =>
      Paginator.pages(client, Uri.unsafeFromString(s"$apiBase/items"), Map.empty, linkHeader)
        .map(_.hcursor.get[Int]("page").toOption.get).compile.toList
    }.attempt.unsafeRunSync()

  test("with credentials, a next link to another origin is refused and never requested") {
    firstPageLinkingTo(s"http://localhost:${other.port()}/steal?x=1")

    val result = walk(bearer)

    assert(result.isLeft, s"expected the walk to fail, got $result")
    val message = result.left.toOption.get.getMessage
    assert(message.contains(s"http://localhost:${other.port()}"), message)
    assert(!message.contains("secret-token"), "the error must not repeat the credentials")
    other.verify(0, anyRequestedFor(anyUrl()))
  }

  test("with credentials, a next link on the same origin is followed") {
    firstPageLinkingTo(s"$apiBase/items?page=2")
    api.stubFor(get(urlPathEqualTo("/items")).withQueryParam("page", equalTo("2"))
      .withHeader("Authorization", equalTo("Bearer secret-token")).willReturn(okJson("""{"page": 2}""")))

    assertEquals(walk(bearer), Right(List(1, 2)))
  }

  test("with credentials, a next link that only changes the scheme is refused") {
    // Same host and port, different scheme: only the scheme part of the origin check can
    // catch this. The test server speaks plain HTTP, so the link goes HTTP to HTTPS; the
    // check is the same in the HTTPS-to-HTTP direction. Without it, the walk would follow
    // the link and fail on TLS instead of refusing it.
    firstPageLinkingTo(s"https://localhost:${api.port()}/items?page=2")

    val result = walk(bearer)

    assert(result.isLeft, s"expected the walk to fail, got $result")
    val message = result.left.toOption.get.getMessage
    assert(message.contains("different origin"), s"failed for another reason: $message")
  }

  test("a relative next link is resolved against the request and followed") {
    firstPageLinkingTo("/items?page=2")
    // Resolved onto the same origin, so the credentials go with it.
    api.stubFor(get(urlPathEqualTo("/items")).withQueryParam("page", equalTo("2"))
      .withHeader("Authorization", equalTo("Bearer secret-token"))
      .willReturn(okJson("""{"page": 2}""")))

    assertEquals(walk(bearer), Right(List(1, 2)))
  }

  test("without credentials, a next link to another origin is followed") {
    // Nothing secret is sent, so a paginated API that hands off to a CDN still works.
    firstPageLinkingTo(s"http://localhost:${other.port()}/page2")

    assertEquals(walk(none), Right(List(1, 2)))
  }
}

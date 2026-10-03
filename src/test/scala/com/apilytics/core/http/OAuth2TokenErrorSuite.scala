package com.apilytics.core.http

import cats.effect.unsafe.implicits.global
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite

import scala.concurrent.duration._

/** A token response without `access_token` doesn't put the token in the error (#315). */
class OAuth2TokenErrorSuite extends FunSuite {

  private var server: WireMockServer = _

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
  }

  override def afterEach(context: AfterEach): Unit = server.stop()

  test("an IdP answering accessToken gets an error naming the fields, not their values") {
    // Not a pattern scrubTokens knows: an opaque value under a camelCase key.
    val secret = "opaque0123456789abcdef"
    server.stubFor(post(urlPathEqualTo("/token"))
      .willReturn(okJson(s"""{"accessToken": "$secret", "expires_in": 3600}""")))

    val error = OAuth2TokenManager
      .resource("id", "client-secret", s"http://localhost:${server.port()}/token", 5.seconds)
      .use(_.getToken)
      .attempt
      .unsafeRunSync()
      .swap
      .getOrElse(fail("a response without access_token was accepted"))

    val shown = error.getMessage + error.toString
    assert(!shown.contains(secret), s"the token reached the error: $shown")
    assert(shown.contains("missing access_token") && shown.contains("accessToken"), shown)
  }
}

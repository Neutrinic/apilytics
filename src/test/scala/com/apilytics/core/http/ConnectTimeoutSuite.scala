package com.apilytics.core.http

import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig}
import munit.FunSuite
import org.http4s.Uri

import scala.concurrent.duration._

/** A request to an unreachable host fails within the configured timeout.
  *
  * On EMR Serverless, which has no internet access without a VPC, each attempt to reach a
  * public API waited for the operating system's TCP connect timeout — about two minutes on
  * Linux — rather than `http.timeout`, and with retries a task took 14 minutes to fail.
  * The connection phase has to be bounded by the same timeout as the request.
  */
class ConnectTimeoutSuite extends FunSuite {

  // Non-routable: packets to it are dropped, so a connect attempt waits rather than
  // being refused. That is what an egress-less network looks like to the client.
  private val blackHole = Uri.unsafeFromString("http://10.255.255.1:81/unreachable")

  test("an unreachable host fails within the configured timeout") {
    val cfg = HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 2.seconds)
    val started = System.nanoTime()
    val outcome = Client.resource(cfg, AuthConfig(authType = AuthType.None))
      .use(_.get(blackHole))
      .attempt
      .unsafeRunTimed(60.seconds)
    val tookMs = (System.nanoTime() - started) / 1000000

    assert(outcome.exists(_.isLeft), s"expected a failure, got $outcome")
    assert(tookMs < 8000, s"took ${tookMs}ms with a 2s timeout: the connect phase is not bounded")
  }

  test("the failure names the host it could not reach") {
    val cfg = HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 2.seconds)
    val err = Client.resource(cfg, AuthConfig(authType = AuthType.None))
      .use(_.get(blackHole, Map("api_key" -> "secret")))
      .attempt
      .unsafeRunTimed(60.seconds)
      .flatMap(_.left.toOption)
      .getOrElse(fail("expected a failure"))

    assert(err.getMessage.contains("10.255.255.1:81/unreachable"), err.getMessage)
    assert(!err.getMessage.contains("secret"), "query parameters must not appear in the error")
  }

  test("an unreachable OAuth2 token endpoint fails within the configured timeout") {
    val started = System.nanoTime()
    val outcome = OAuth2TokenManager.resource("id", "secret", "http://10.255.255.1:81/token", 2.seconds)
      .use(_.getToken)
      .attempt
      .unsafeRunTimed(60.seconds)
    val tookMs = (System.nanoTime() - started) / 1000000

    assert(outcome.exists(_.isLeft), s"expected a failure, got $outcome")
    assert(tookMs < 8000, s"took ${tookMs}ms with a 2s timeout")
  }
}

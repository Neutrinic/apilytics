package com.apilytics.core.http

import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.stubbing.Scenario
import munit.FunSuite
import org.http4s.Uri

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Every attempt takes a rate-limit permit, retries included (#279).
  *
  * The permit used to be taken once, before the first attempt. A burst of 429 or 5xx
  * responses was then retried as fast as the backoff allowed, above `rate-limit`, which
  * is exactly when an API is asking to be sent less.
  */
class RateLimitRetrySuite extends FunSuite {

  private var server: WireMockServer = _

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
  }

  override def afterEach(context: AfterEach): Unit = server.stop()

  /** Gaps between the requests the server received, in milliseconds. */
  private def gapsMs: List[Long] = {
    val times = server.getAllServeEvents.asScala.toList.map(_.getRequest.getLoggedDate.getTime).sorted
    times.zip(times.drop(1)).map { case (a, b) => b - a }
  }

  private def stubFailingThenOk(status: Int): Unit = {
    server.stubFor(get(urlPathEqualTo("/items")).inScenario("flaky")
      .whenScenarioStateIs(Scenario.STARTED).willReturn(aResponse().withStatus(status))
      .willSetStateTo("failed-once"))
    server.stubFor(get(urlPathEqualTo("/items")).inScenario("flaky")
      .whenScenarioStateIs("failed-once").willReturn(aResponse().withStatus(status))
      .willSetStateTo("failed-twice"))
    server.stubFor(get(urlPathEqualTo("/items")).inScenario("flaky")
      .whenScenarioStateIs("failed-twice").willReturn(okJson("""{"ok": true}""")))
  }

  // 2 requests a second: attempts must be at least 500ms apart. Backoff is capped at zero,
  // so any spacing comes from the limiter alone.
  private val http = HttpConfig(maxRetries = 3, maxBackoff = 0.seconds, timeout = 5.seconds, rateLimit = Some(2))

  private def fetch(): Int =
    Client.resource(http, AuthConfig(authType = AuthType.None)).use { client =>
      client.get(Uri.unsafeFromString(s"http://localhost:${server.port()}/items"))
    }.unsafeRunSync().status

  for (status <- Seq(503, 429)) {
    test(s"retries after $status wait for a rate-limit permit") {
      stubFailingThenOk(status)

      assertEquals(fetch(), 200)
      val gaps = gapsMs
      assertEquals(gaps.size, 2, s"expected three attempts, got gaps $gaps")
      // Three attempts at 2 a second span at least two 500ms intervals. Measured where the
      // server logs them, so a first attempt slowed by connection setup can make one gap
      // look shorter; the span is what the limit guarantees. Unthrottled, it was ~30ms.
      assert(gaps.sum >= 850, s"retries were sent ${gaps.mkString("ms, ")}ms apart, faster than 2 a second")
      assert(gaps.forall(_ >= 300), s"a retry was sent ${gaps.min}ms after the previous attempt")
    }
  }
}

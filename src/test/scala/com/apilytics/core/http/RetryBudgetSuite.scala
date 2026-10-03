package com.apilytics.core.http

import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig, ResponseFormat}
import munit.FunSuite
import org.http4s.Uri

import java.net.{ServerSocket, SocketException}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

/** `max-retries` has to mean what it says (#245).
  *
  * Ember retries internally by default, underneath both our retry loop and the rate
  * limiter. The effect multiplied: attempts came out at 3 × (max-retries + 1), and because
  * `rateLimiter.acquire` runs once per attempt *we* make, the extra requests never reached
  * the limiter at all. A source configured for 60 requests per hour could issue 180
  * against a flaky connection — which, for the APIs this connector targets, is the
  * difference between working and being blocked.
  *
  * Counting connections at a socket is the only way to see this; nothing above ember
  * observes the duplicates.
  */
class RetryBudgetSuite extends FunSuite {

  /** Accepts, then resets — every attempt fails, so all of them are counted. */
  private def connectionsFor(maxRetries: Int): Int = {
    val connections = new AtomicInteger(0)
    val server = new ServerSocket(0)
    val t = new Thread(() => {
      try while (!server.isClosed) {
        val sock = server.accept()
        connections.incrementAndGet()
        try {
          sock.getInputStream.read(new Array[Byte](4096))
          sock.setSoLinger(true, 0)
        } catch { case _: SocketException => () }
        finally sock.close()
      } catch { case _: Throwable => () }
    })
    t.setDaemon(true)
    t.start()

    val cfg = HttpConfig(maxRetries = maxRetries, maxBackoff = 500.millis,
                         timeout = 5.seconds, responseFormat = ResponseFormat.NDJSON)
    try {
      Client.resource(cfg, AuthConfig(authType = AuthType.None)).use { client =>
        client
          .getStreaming(Uri.unsafeFromString(s"http://localhost:${server.getLocalPort}/x"),
                        Map.empty, ResponseFormat.NDJSON)
          .compile.toList.attempt
      }.timeout(60.seconds).unsafeRunSync()
    } finally server.close()

    connections.get()
  }

  /** Answers the first request with a 500, then resets every connection after it. */
  private def requestsAfterServerErrorThenResets(maxRetries: Int, format: ResponseFormat = ResponseFormat.Json): Int = {
    val requests = new AtomicInteger(0)
    val server = new ServerSocket(0)
    val t = new Thread(() => {
      try while (!server.isClosed) {
        val sock = server.accept()
        val n = requests.incrementAndGet()
        try {
          sock.getInputStream.read(new Array[Byte](4096))
          if (n == 1) {
            sock.getOutputStream.write(
              "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".getBytes
            )
            sock.getOutputStream.flush()
          } else sock.setSoLinger(true, 0)
        } catch { case _: SocketException => () }
        finally sock.close()
      } catch { case _: Throwable => () }
    })
    t.setDaemon(true)
    t.start()

    val cfg = HttpConfig(maxRetries = maxRetries, maxBackoff = 50.millis, timeout = 5.seconds, responseFormat = format)
    try {
      Client.resource(cfg, AuthConfig(authType = AuthType.None)).use { client =>
        client
          .getStreaming(Uri.unsafeFromString(s"http://localhost:${server.getLocalPort}/x"), Map.empty, format)
          .compile.toList.attempt
      }.timeout(60.seconds).unsafeRunSync()
    } finally server.close()

    requests.get()
  }

  test("server errors and network errors share one retry budget (#313)") {
    // The 5xx retry ran inside the attempt that got the 500, so when it then hit a network
    // error the outer handler restarted the whole chain: 3/5/7/11 requests for max-retries
    // 1/2/3/5, against budgets of 2/3/4/6.
    for (format <- List(ResponseFormat.Json, ResponseFormat.NDJSON); maxRetries <- List(1, 2, 3, 5)) {
      assertEquals(
        requestsAfterServerErrorThenResets(maxRetries, format), maxRetries + 1,
        s"$format, max-retries=$maxRetries"
      )
    }
  }

  test("a request is attempted exactly max-retries + 1 times") {
    for (maxRetries <- List(0, 1, 2)) {
      assertEquals(
        connectionsFor(maxRetries), maxRetries + 1,
        s"max-retries=$maxRetries issued the wrong number of requests; anything above the " +
          "budget also escapes the rate limiter, which only counts our own attempts"
      )
    }
  }

  test("a Retry-After date is honoured by streamed requests as by full-body ones") {
    // Streams read only a number of seconds, so a date fell back to the (short) backoff and
    // retried before the server allowed.
    for (format <- List(ResponseFormat.Json, ResponseFormat.NDJSON)) {
      val server = new com.github.tomakehurst.wiremock.WireMockServer(
        com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig().dynamicPort()
      )
      server.start()
      try {
        import com.github.tomakehurst.wiremock.client.WireMock._
        import com.github.tomakehurst.wiremock.stubbing.Scenario
        val retryAt = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC).plusSeconds(3)
          .format(java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME)
        server.stubFor(get(urlPathEqualTo("/x")).inScenario("429").whenScenarioStateIs(Scenario.STARTED)
          .willReturn(aResponse().withStatus(429).withHeader("Retry-After", retryAt))
          .willSetStateTo("ok"))
        server.stubFor(get(urlPathEqualTo("/x")).inScenario("429").whenScenarioStateIs("ok")
          .willReturn(okJson("""{"id": 1}""")))

        val cfg = HttpConfig(maxRetries = 1, maxBackoff = 10.millis, timeout = 5.seconds, responseFormat = format)
        val started = System.nanoTime()
        Client.resource(cfg, AuthConfig(authType = AuthType.None)).use { client =>
          client.getStreaming(Uri.unsafeFromString(s"http://localhost:${server.port()}/x"), Map.empty, format)
            .compile.toList
        }.timeout(30.seconds).unsafeRunSync()
        val waited = (System.nanoTime() - started) / 1000000

        // The date has whole-second precision, so the wait is at least about two seconds.
        assert(waited >= 1500, s"$format retried after ${waited}ms, before the Retry-After date")
      } finally server.stop()
    }
  }

  test("a Retry-After too large for a duration falls back to backoff instead of failing") {
    // `Long.MaxValue.seconds` throws: the request failed rather than retrying.
    val server = new com.github.tomakehurst.wiremock.WireMockServer(
      com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig().dynamicPort()
    )
    server.start()
    try {
      import com.github.tomakehurst.wiremock.client.WireMock._
      import com.github.tomakehurst.wiremock.stubbing.Scenario
      server.stubFor(get(urlPathEqualTo("/x")).inScenario("429").whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(429).withHeader("Retry-After", Long.MaxValue.toString))
        .willSetStateTo("ok"))
      server.stubFor(get(urlPathEqualTo("/x")).inScenario("429").whenScenarioStateIs("ok")
        .willReturn(okJson("""{"id": 1}""")))

      // Backoff is in whole seconds: the first retry waits 1 s, under a 2 s max-backoff.
      val cfg = HttpConfig(maxRetries = 1, maxBackoff = 2.seconds, timeout = 5.seconds)
      val started = System.nanoTime()
      val response = Client.resource(cfg, AuthConfig(authType = AuthType.None)).use { client =>
        client.get(Uri.unsafeFromString(s"http://localhost:${server.port()}/x"))
      }.timeout(30.seconds).unsafeRunSync()
      val waited = (System.nanoTime() - started) / 1000000

      assertEquals(response.status, 200)
      assert(waited >= 900, s"retried after ${waited}ms, not after the 1 s backoff")
    } finally server.stop()
  }
}

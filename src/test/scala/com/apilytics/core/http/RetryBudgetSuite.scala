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

  test("a request is attempted exactly max-retries + 1 times") {
    for (maxRetries <- List(0, 1, 2)) {
      assertEquals(
        connectionsFor(maxRetries), maxRetries + 1,
        s"max-retries=$maxRetries issued the wrong number of requests; anything above the " +
          "budget also escapes the rate limiter, which only counts our own attempts"
      )
    }
  }
}

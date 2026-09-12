package com.apilytics.core.http

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig, ResponseFormat}
import munit.FunSuite
import org.http4s.Uri

import java.io.OutputStream
import java.net.{ServerSocket, SocketException}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

/** Retry scope for streaming responses (#243).
  *
  * A retry re-issues the request from the beginning — there is no resume protocol — so
  * once a byte has reached the consumer, retrying stops being recovery and becomes silent
  * duplication, splicing a half-written record onto a fresh response.
  *
  * Two things currently prevent that, and only one of them is deliberate. Ember reports a
  * connection dying mid-body as `ReachedEndOfStream`, which `isTransientNetworkError` does
  * not match, so the retry branch is not reached today. That is an accident of ember's
  * error types, not a decision: widening the transient list — adding `IOException`, say —
  * would make the replay reachable. The `emitted` guard in `streamBodyWithRetry` is what
  * makes such a change safe, and this test is what would catch its removal.
  */
class MidStreamRetrySuite extends FunSuite {

  /** Serves valid records, then resets the connection with RST rather than FIN. */
  private def resettingServer(connections: AtomicInteger): ServerSocket = {
    val server = new ServerSocket(0)
    val t = new Thread(() => {
      try while (!server.isClosed) {
        val sock = server.accept()
        connections.incrementAndGet()
        try {
          val in = sock.getInputStream
          in.read(new Array[Byte](4096)) // consume the request head

          val out: OutputStream = sock.getOutputStream
          // Content-Length promises far more than is written, so the body is truncated.
          out.write(("HTTP/1.1 200 OK\r\n" +
                     "Content-Type: application/x-ndjson\r\n" +
                     "Content-Length: 1000\r\n\r\n").getBytes)
          out.write("{\"id\":1}\n{\"id\":2}\n".getBytes)
          out.flush()
          Thread.sleep(50)
          sock.setSoLinger(true, 0)
        } catch { case _: SocketException => () }
        finally sock.close()
      } catch { case _: Throwable => () }
    })
    t.setDaemon(true)
    t.start()
    server
  }

  test("a connection dying mid-body surfaces an error and is not re-issued") {
    val connections = new AtomicInteger(0)
    val server = resettingServer(connections)

    try {
      val httpConfig = HttpConfig(
        maxRetries = 3, // retries are available, so any retry shows up as a 2nd connection
        maxBackoff = 1.second,
        timeout = 10.seconds,
        responseFormat = ResponseFormat.NDJSON
      )

      val result = Client.resource(httpConfig, AuthConfig(authType = AuthType.None)).use { client =>
        client
          .getStreaming(Uri.unsafeFromString(s"http://localhost:${server.getLocalPort}/x"),
                        Map.empty, ResponseFormat.NDJSON)
          .compile
          .toList
          .attempt
      }.timeout(30.seconds).unsafeRunSync()

      assert(result.isLeft, s"a truncated body must not look like a complete read: got $result")
      assertEquals(
        connections.get(), 1,
        s"request was re-issued ${connections.get()} times after records had been " +
          "delivered; re-issuing replays data the consumer already holds"
      )
    } finally server.close()
  }
}

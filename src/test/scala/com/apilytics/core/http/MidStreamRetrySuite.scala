package com.apilytics.core.http

import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig, ResponseFormat}
import munit.FunSuite
import org.http4s.Uri

import java.net.{ServerSocket, SocketException}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

/** Retry scope for streaming responses (#243).
  *
  * A retry re-issues the request from the start — there is no resume protocol — so the
  * moment a record reaches the consumer, retrying stops being recovery and becomes silent
  * duplication, splicing a partial record onto a fresh response for NDJSON and SSE.
  *
  * Retrying before anything was emitted must keep working, though, or a blip while
  * connecting turns into a failed query. Both halves are pinned here.
  *
  * This needs a socket rather than WireMock, whose faults replace the response instead of
  * killing it partway. It also needs the reset to arrive while the client is blocked on a
  * read: reset immediately after writing and the bytes are already buffered, so the client
  * reports `ReachedEndOfStream` — not classified transient — and never reaches the retry
  * branch at all. That timing difference is why an earlier version of this test passed
  * whether or not the guard was present.
  */
class MidStreamRetrySuite extends FunSuite {

  private val head =
    "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nContent-Length: 1000\r\n\r\n"
  private val body = "{\"id\":1}\n{\"id\":2}\n"

  /** Serves `writeBody` bytes (if any), waits for the client to block, then sends RST. */
  private def resettingServer(connections: AtomicInteger, writeBody: Boolean): ServerSocket = {
    val server = new ServerSocket(0)
    val t = new Thread(() => {
      try while (!server.isClosed) {
        val sock = server.accept()
        connections.incrementAndGet()
        try {
          sock.getInputStream.read(new Array[Byte](4096)) // consume the request head
          if (writeBody) {
            val out = sock.getOutputStream
            out.write(head.getBytes)
            out.write(body.getBytes)
            out.flush()
            // Long enough for the client to consume both records and block on the next
            // read, so the reset lands as SocketException rather than a buffered EOF.
            Thread.sleep(1500)
          }
          sock.setSoLinger(true, 0) // RST, not FIN
        } catch { case _: SocketException => () }
        finally sock.close()
      } catch { case _: Throwable => () }
    })
    t.setDaemon(true)
    t.start()
    server
  }

  private def read(port: Int, maxRetries: Int) = {
    val cfg = HttpConfig(maxRetries = maxRetries, maxBackoff = 1.second, timeout = 30.seconds,
                         responseFormat = ResponseFormat.NDJSON)
    Client.resource(cfg, AuthConfig(authType = AuthType.None)).use { client =>
      client
        .getStreaming(Uri.unsafeFromString(s"http://localhost:$port/x"), Map.empty,
                      ResponseFormat.NDJSON)
        .compile.toList.attempt
    }.timeout(60.seconds).unsafeRunSync()
  }

  test("a reset after records have been delivered is not retried") {
    val connections = new AtomicInteger(0)
    val server = resettingServer(connections, writeBody = true)
    try {
      val result = read(server.getLocalPort, maxRetries = 2)

      assert(result.isLeft, s"a reset mid-body must not look like a complete read: got $result")
      assertEquals(
        connections.get(), 1,
        s"request was issued ${connections.get()} times after records had been delivered; " +
          "re-issuing replays records the consumer already holds"
      )
    } finally server.close()
  }

  test("a reset before anything is delivered is still retried") {
    // The guard must not cost us ordinary transient recovery.
    val connections = new AtomicInteger(0)
    val server = resettingServer(connections, writeBody = false)
    try {
      val result = read(server.getLocalPort, maxRetries = 2)

      assert(result.isLeft, "every attempt failed, so the read should fail")
      assert(
        connections.get() > 1,
        s"only ${connections.get()} attempt(s); a failure before any record must be retried"
      )
    } finally server.close()
  }
}

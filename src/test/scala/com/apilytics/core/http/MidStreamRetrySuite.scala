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
  * The line is the first *body byte*, not the response. Failing before anything is
  * emitted must still retry, including after the headers have arrived: nothing has
  * reached the consumer at that point, so re-issuing costs a request and loses nothing.
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

  private sealed trait Serve
  private case object Nothing extends Serve
  private case object HeadOnly extends Serve
  private case object HeadAndRecords extends Serve

  /** Writes as much as `serve` says, waits for the client to block, then sends RST. */
  private def resettingServer(connections: AtomicInteger, serve: Serve): ServerSocket = {
    val server = new ServerSocket(0)
    val t = new Thread(() => {
      try while (!server.isClosed) {
        val sock = server.accept()
        connections.incrementAndGet()
        try {
          sock.getInputStream.read(new Array[Byte](4096)) // consume the request head
          if (serve != Nothing) {
            val out = sock.getOutputStream
            out.write(head.getBytes)
            if (serve == HeadAndRecords) out.write(body.getBytes)
            out.flush()
            // Long enough for the client to consume whatever was sent and block on the
            // next read, so the reset lands as SocketException rather than a buffered EOF.
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
    }.timeout(90.seconds).unsafeRunSync()
  }

  test("a reset after records have been delivered is not retried") {
    val connections = new AtomicInteger(0)
    val server = resettingServer(connections, HeadAndRecords)
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

  test("a reset after the headers but before any record is still retried") {
    // The boundary that matters. Stopping at the response rather than at the first body
    // byte would also pass the two tests either side of this one, while giving up a retry
    // that costs nothing — no record has reached the consumer yet.
    val connections = new AtomicInteger(0)
    val server = resettingServer(connections, HeadOnly)
    try {
      val result = read(server.getLocalPort, maxRetries = 2)

      assert(result.isLeft, "every attempt failed, so the read should fail")
      assertEquals(
        connections.get(), 3,
        s"only ${connections.get()} attempt(s); the headers arriving is not the consumer " +
          "receiving anything, so this is still safe to retry"
      )
    } finally server.close()
  }

  test("a reset before any response is retried") {
    val connections = new AtomicInteger(0)
    val server = resettingServer(connections, Nothing)
    try {
      val result = read(server.getLocalPort, maxRetries = 2)

      assert(result.isLeft, "every attempt failed, so the read should fail")
      assertEquals(connections.get(), 3, "a failure while connecting must be retried")
    } finally server.close()
  }
}

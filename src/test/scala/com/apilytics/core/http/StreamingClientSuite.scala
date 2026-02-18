package com.apilytics.core.http

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig, ResponseFormat}
import munit.FunSuite
import org.http4s.Uri

import scala.concurrent.duration._

/** Integration tests for streaming HTTP client against public endpoints.
  *
  * These tests are marked .ignore for CI since they depend on external services.
  * Run manually: sbt "testOnly com.apilytics.core.http.StreamingClientSuite"
  * Then remove .ignore temporarily to run.
  */
class StreamingClientSuite extends FunSuite {

  // SSE test against sse.dev - sends JSON events every 2 seconds
  test("SSE streaming from sse.dev".ignore) {
    val httpConfig = HttpConfig(
      maxRetries = 3,
      maxBackoff = 30.seconds,
      timeout = 60.seconds,  // Longer timeout for streaming
      responseFormat = ResponseFormat.SSE
    )
    val authConfig = AuthConfig(authType = AuthType.None)

    println("Starting SSE test...")

    // Test that at least ONE event is received and parsed correctly
    // (sse.dev should send events every 2 seconds but the stream might be slow)
    val result = Client.resource(httpConfig, authConfig).use { client =>
      val uri = Uri.unsafeFromString("https://sse.dev/test")

      println(s"Connecting to $uri")

      client.getStreaming(uri, Map.empty, ResponseFormat.SSE)
        .evalTap(json => IO(println(s"Received: $json")))
        .head  // Just take the first event
        .compile
        .lastOrError
    }.timeout(15.seconds).unsafeRunSync()

    println(s"Got event: $result")
    assert(result.isObject, s"Expected JSON object, got: $result")
    // sse.dev returns {"testing":true,"sse_dev":"is great","msg":"It works!","now":...}
    assertEquals(result.hcursor.get[Boolean]("testing").toOption, Some(true))
    assertEquals(result.hcursor.get[String]("msg").toOption, Some("It works!"))
  }

  // Test NDJSON against Lichess public API
  // https://lichess.org/api/tv/feed streams live chess moves as NDJSON
  test("NDJSON streaming from Lichess API".ignore) {
    val httpConfig = HttpConfig(
      maxRetries = 3,
      maxBackoff = 30.seconds,
      timeout = 30.seconds,
      responseFormat = ResponseFormat.NDJSON
    )
    val authConfig = AuthConfig(authType = AuthType.None)

    println("Starting NDJSON test against Lichess...")

    val result = Client.resource(httpConfig, authConfig).use { client =>
      val uri = Uri.unsafeFromString("https://lichess.org/api/tv/feed")

      println(s"Connecting to $uri")

      client.getStreaming(uri, Map.empty, ResponseFormat.NDJSON)
        .evalTap(json => IO(println(s"Received: ${json.noSpaces.take(100)}...")))
        .take(3)  // Take 3 events
        .compile
        .toList
    }.timeout(15.seconds).unsafeRunSync()

    println(s"Got ${result.size} NDJSON records")
    assertEquals(result.size, 3)
    // Lichess returns objects with "t" (type) and "d" (data) fields
    result.foreach { json =>
      assert(json.isObject, s"Expected JSON object, got: $json")
      assert(json.hcursor.get[String]("t").isRight, s"Expected 't' field in: $json")
    }
  }
}

package com.apilytics.core.http

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig, ResponseFormat}
import munit.FunSuite
import org.http4s.Uri

import scala.concurrent.duration._

/** Integration tests for streaming HTTP client against public endpoints. */
class StreamingClientSuite extends FunSuite {

  // sse.dev is a public SSE test endpoint
  // https://sse.dev/test sends a JSON message every 2 seconds by default
  test("SSE streaming from sse.dev".ignore) {
    val httpConfig = HttpConfig(
      maxRetries = 3,
      maxBackoff = 30.seconds,
      timeout = 30.seconds,
      responseFormat = ResponseFormat.SSE
    )
    val authConfig = AuthConfig(authType = AuthType.None)

    Client.resource(httpConfig, authConfig).use { client =>
      val uri = Uri.unsafeFromString("https://sse.dev/test")

      // Take 3 events (6 seconds at default 2s interval)
      client.getStreaming(uri, Map.empty, ResponseFormat.SSE)
        .take(3)
        .compile
        .toList
        .flatMap { results =>
          IO {
            println(s"Received ${results.size} SSE events:")
            results.foreach(json => println(s"  $json"))

            assertEquals(results.size, 3)
            // sse.dev returns JSON with at least a timestamp
            results.foreach { json =>
              assert(json.isObject, s"Expected JSON object, got: $json")
            }
          }
        }
    }.unsafeRunSync()
  }

  // Test SSE against public endpoint
  // Run manually: sbt "testOnly com.apilytics.core.http.StreamingClientSuite"
  // Then remove .ignore temporarily to run
  test("SSE streaming from sse.dev public endpoint".ignore) {
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

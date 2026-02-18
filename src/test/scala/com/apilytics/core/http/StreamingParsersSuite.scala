package com.apilytics.core.http

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import munit.FunSuite

class StreamingParsersSuite extends FunSuite {

  // ==================== NDJSON Tests ====================

  test("NDJSON parses multiple JSON objects") {
    val input = """{"name": "Alice"}
{"name": "Bob"}
{"name": "Charlie"}"""

    val bytes = input.getBytes("UTF-8")
    val results = Stream.emits(bytes).through(StreamingParsers.ndjson).compile.toList.unsafeRunSync()

    assertEquals(results.size, 3)
    assertEquals(results(0).hcursor.get[String]("name").toOption, Some("Alice"))
    assertEquals(results(1).hcursor.get[String]("name").toOption, Some("Bob"))
    assertEquals(results(2).hcursor.get[String]("name").toOption, Some("Charlie"))
  }

  test("NDJSON skips empty lines") {
    val input = """{"a": 1}

{"b": 2}

"""

    val bytes = input.getBytes("UTF-8")
    val results = Stream.emits(bytes).through(StreamingParsers.ndjson).compile.toList.unsafeRunSync()

    assertEquals(results.size, 2)
  }

  test("NDJSON handles arrays") {
    val input = """[1, 2, 3]
[4, 5, 6]"""

    val bytes = input.getBytes("UTF-8")
    val results = Stream.emits(bytes).through(StreamingParsers.ndjson).compile.toList.unsafeRunSync()

    assertEquals(results.size, 2)
    assertEquals(results(0).asArray.map(_.size), Some(3))
  }

  test("NDJSON fails on invalid JSON") {
    val input = """{"valid": true}
not valid json
{"also": "valid"}"""

    val bytes = input.getBytes("UTF-8")
    val result = Stream.emits(bytes).through(StreamingParsers.ndjson).compile.toList.attempt.unsafeRunSync()

    assert(result.isLeft)
    assert(result.swap.toOption.get.getMessage.contains("Invalid JSON"))
  }

  // ==================== SSE Tests ====================

  test("SSE parses simple data events") {
    val input = """data: {"message": "hello"}

data: {"message": "world"}

"""

    val bytes = input.getBytes("UTF-8")
    val results = Stream.emits(bytes).through(StreamingParsers.sse).compile.toList.unsafeRunSync()

    assertEquals(results.size, 2)
    assertEquals(results(0).hcursor.get[String]("message").toOption, Some("hello"))
    assertEquals(results(1).hcursor.get[String]("message").toOption, Some("world"))
  }

  test("SSE ignores comments") {
    val input = """: this is a comment
data: {"value": 1}

: another comment
data: {"value": 2}

"""

    val bytes = input.getBytes("UTF-8")
    val results = Stream.emits(bytes).through(StreamingParsers.sse).compile.toList.unsafeRunSync()

    assertEquals(results.size, 2)
  }

  test("SSE handles multi-line data") {
    val input = """data: {"line": 1,
data:  "continued": true}

"""

    val bytes = input.getBytes("UTF-8")
    val results = Stream.emits(bytes).through(StreamingParsers.sse).compile.toList.unsafeRunSync()

    assertEquals(results.size, 1)
    assertEquals(results(0).hcursor.get[Int]("line").toOption, Some(1))
    assertEquals(results(0).hcursor.get[Boolean]("continued").toOption, Some(true))
  }

  test("SSE handles event type field (ignored for JSON extraction)") {
    val input = """event: update
data: {"type": "update"}

event: delete
data: {"type": "delete"}

"""

    val bytes = input.getBytes("UTF-8")
    val results = Stream.emits(bytes).through(StreamingParsers.sse).compile.toList.unsafeRunSync()

    assertEquals(results.size, 2)
    assertEquals(results(0).hcursor.get[String]("type").toOption, Some("update"))
    assertEquals(results(1).hcursor.get[String]("type").toOption, Some("delete"))
  }

  test("SSE skips events without data") {
    val input = """event: ping

data: {"value": 1}

"""

    val bytes = input.getBytes("UTF-8")
    val results = Stream.emits(bytes).through(StreamingParsers.sse).compile.toList.unsafeRunSync()

    assertEquals(results.size, 1)
    assertEquals(results(0).hcursor.get[Int]("value").toOption, Some(1))
  }
}

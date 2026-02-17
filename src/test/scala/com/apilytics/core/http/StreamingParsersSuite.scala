package com.apilytics.core.http

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import io.circe.Json
import io.circe.parser.parse
import munit.FunSuite
import org.msgpack.core.MessagePack

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

  // ==================== MessagePack Tests ====================

  test("MessagePack parses object") {
    val packer = MessagePack.newDefaultBufferPacker()
    packer.packMapHeader(2)
    packer.packString("name")
    packer.packString("Alice")
    packer.packString("age")
    packer.packInt(30)
    val bytes = packer.toByteArray
    packer.close()

    val results = Stream.emits(bytes).through(StreamingParsers.msgpack).compile.toList.unsafeRunSync()

    assertEquals(results.size, 1)
    assertEquals(results(0).hcursor.get[String]("name").toOption, Some("Alice"))
    assertEquals(results(0).hcursor.get[Int]("age").toOption, Some(30))
  }

  test("MessagePack parses array") {
    val packer = MessagePack.newDefaultBufferPacker()
    packer.packArrayHeader(3)
    packer.packInt(1)
    packer.packInt(2)
    packer.packInt(3)
    val bytes = packer.toByteArray
    packer.close()

    val results = Stream.emits(bytes).through(StreamingParsers.msgpack).compile.toList.unsafeRunSync()

    assertEquals(results.size, 1)
    assertEquals(results(0).asArray.map(_.size), Some(3))
  }

  test("MessagePack parses primitives") {
    val packer = MessagePack.newDefaultBufferPacker()
    packer.packString("hello")
    packer.packInt(42)
    packer.packBoolean(true)
    packer.packDouble(3.14)
    packer.packNil()
    val bytes = packer.toByteArray
    packer.close()

    val results = Stream.emits(bytes).through(StreamingParsers.msgpack).compile.toList.unsafeRunSync()

    assertEquals(results.size, 5)
    assertEquals(results(0).asString, Some("hello"))
    assertEquals(results(1).asNumber.flatMap(_.toInt), Some(42))
    assertEquals(results(2).asBoolean, Some(true))
    assert(results(3).asNumber.exists(n => math.abs(n.toDouble - 3.14) < 0.001))
    assert(results(4).isNull)
  }

  test("MessagePack parses nested structures") {
    val packer = MessagePack.newDefaultBufferPacker()
    packer.packMapHeader(1)
    packer.packString("user")
    packer.packMapHeader(2)
    packer.packString("name")
    packer.packString("Bob")
    packer.packString("tags")
    packer.packArrayHeader(2)
    packer.packString("admin")
    packer.packString("active")
    val bytes = packer.toByteArray
    packer.close()

    val results = Stream.emits(bytes).through(StreamingParsers.msgpack).compile.toList.unsafeRunSync()

    assertEquals(results.size, 1)
    val user = results(0).hcursor.downField("user")
    assertEquals(user.get[String]("name").toOption, Some("Bob"))
    val tags = user.downField("tags").as[List[String]].toOption
    assertEquals(tags, Some(List("admin", "active")))
  }

  test("MessagePack handles binary as base64") {
    val packer = MessagePack.newDefaultBufferPacker()
    val binaryData = Array[Byte](0x01, 0x02, 0x03, 0x04)
    packer.packBinaryHeader(binaryData.length)
    packer.writePayload(binaryData)
    val bytes = packer.toByteArray
    packer.close()

    val results = Stream.emits(bytes).through(StreamingParsers.msgpack).compile.toList.unsafeRunSync()

    assertEquals(results.size, 1)
    // Binary data should be base64 encoded
    val encoded = results(0).asString
    assert(encoded.isDefined)
    val decoded = java.util.Base64.getDecoder.decode(encoded.get)
    assertEquals(decoded.toList, binaryData.toList)
  }

  test("MessagePack parses multiple concatenated values") {
    val packer = MessagePack.newDefaultBufferPacker()
    packer.packMapHeader(1)
    packer.packString("id")
    packer.packInt(1)
    packer.packMapHeader(1)
    packer.packString("id")
    packer.packInt(2)
    packer.packMapHeader(1)
    packer.packString("id")
    packer.packInt(3)
    val bytes = packer.toByteArray
    packer.close()

    val results = Stream.emits(bytes).through(StreamingParsers.msgpack).compile.toList.unsafeRunSync()

    assertEquals(results.size, 3)
    assertEquals(results(0).hcursor.get[Int]("id").toOption, Some(1))
    assertEquals(results(1).hcursor.get[Int]("id").toOption, Some(2))
    assertEquals(results(2).hcursor.get[Int]("id").toOption, Some(3))
  }
}

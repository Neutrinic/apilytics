package com.apilytics.core.http

import org.msgpack.core.MessagePack
import munit.FunSuite
import cats.effect.unsafe.implicits.global
import fs2.Stream

/** Tests for MessagePack edge cases around chunk boundaries and incomplete data. */
class MsgpackCursorBehaviorTest extends FunSuite {

  test("MessagePack handles truncated stream with complete values followed by incomplete") {
    // Create two complete msgpack values followed by one incomplete
    val packer = MessagePack.newDefaultBufferPacker()

    // First complete value
    packer.packMapHeader(1)
    packer.packString("id")
    packer.packInt(1)

    // Second complete value
    packer.packMapHeader(1)
    packer.packString("id")
    packer.packInt(2)

    // Third value (will be truncated)
    packer.packMapHeader(2)
    packer.packString("name")
    packer.packString("Alice")
    packer.packString("age")
    packer.packInt(30)

    val fullBytes = packer.toByteArray
    packer.close()

    // Truncate mid-way through the third value
    val truncatedBytes = fullBytes.take(fullBytes.length - 5)

    // Stream should parse the two complete values and discard the incomplete one
    val results = Stream.emits(truncatedBytes)
      .through(StreamingParsers.msgpack)
      .compile.toList.unsafeRunSync()

    // Should get exactly 2 complete values
    assertEquals(results.size, 2)
    assertEquals(results(0).hcursor.get[Int]("id").toOption, Some(1))
    assertEquals(results(1).hcursor.get[Int]("id").toOption, Some(2))
  }

  test("MessagePack accumulates bytes across chunks for single value") {
    // Create a value that will be split across chunks
    val packer = MessagePack.newDefaultBufferPacker()
    packer.packMapHeader(1)
    packer.packString("name")
    packer.packString("Alice")
    val fullBytes = packer.toByteArray
    packer.close()

    // Split into two chunks
    val splitAt = fullBytes.length / 2
    val chunk1 = fullBytes.take(splitAt)
    val chunk2 = fullBytes.drop(splitAt)

    // Stream with two separate chunks
    val results = Stream.emits(Seq(chunk1, chunk2))
      .flatMap(chunk => Stream.emits(chunk))
      .through(StreamingParsers.msgpack)
      .compile.toList.unsafeRunSync()

    // Should get exactly 1 complete value after both chunks processed
    assertEquals(results.size, 1)
    assertEquals(results(0).hcursor.get[String]("name").toOption, Some("Alice"))
  }
}

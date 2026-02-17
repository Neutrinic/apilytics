package com.apilytics.core.http

import cats.effect.IO
import fs2.{Chunk, Pipe, Pull, Stream}
import io.circe.Json
import io.circe.parser.parse
import org.msgpack.core.{MessagePack, MessageUnpacker}

import java.io.ByteArrayInputStream

/** Parsers for streaming response formats (NDJSON, SSE, MessagePack). */
object StreamingParsers {

  /** Parse newline-delimited JSON (JSON Lines / NDJSON).
    *
    * Each line is a complete JSON object. Empty lines are skipped.
    * Used by: BigQuery export, Elasticsearch scroll, CouchDB changes feed.
    */
  def ndjson: Pipe[IO, Byte, Json] =
    _.through(fs2.text.utf8.decode)
      .through(fs2.text.lines)
      .filter(_.nonEmpty)
      .evalMap(line => IO.fromEither(parse(line).left.map(e => new RuntimeException(s"Invalid JSON: ${e.message}"))))

  /** Server-Sent Event parsed from SSE stream. */
  case class SSEEvent(
      eventType: Option[String],
      data: String,
      id: Option[String]
  )

  /** Parse Server-Sent Events (SSE) stream.
    *
    * SSE format:
    * - Lines starting with ":" are comments (ignored)
    * - "event: <type>" sets event type
    * - "data: <content>" is the payload (can span multiple lines)
    * - "id: <id>" sets the event ID
    * - Empty line terminates an event
    *
    * Multi-line data fields are joined with newlines per SSE spec.
    * The combined data must be valid JSON - if a JSON object is split
    * across multiple `data:` lines, each line should end with a valid
    * continuation (e.g., trailing comma for object fields).
    *
    * Only events with data are emitted. Data is parsed as JSON.
    */
  def sse: Pipe[IO, Byte, Json] =
    _.through(fs2.text.utf8.decode)
      .through(fs2.text.lines)
      .through(parseSSEEvents)
      .filter(_.data.nonEmpty)
      .evalMap(event => IO.fromEither(parse(event.data).left.map(e => new RuntimeException(s"Invalid JSON in SSE data: ${e.message}"))))

  /** Parse raw SSE lines into SSEEvent objects. */
  private def parseSSEEvents: Pipe[IO, String, SSEEvent] = {
    def go(
        s: Stream[IO, String],
        eventType: Option[String],
        dataLines: List[String],
        eventId: Option[String]
    ): Pull[IO, SSEEvent, Unit] =
      s.pull.uncons1.flatMap {
        case None =>
          // End of stream - emit final event if we have data
          if (dataLines.nonEmpty)
            Pull.output1(SSEEvent(eventType, dataLines.reverse.mkString("\n"), eventId))
          else
            Pull.done

        case Some((line, rest)) =>
          if (line.isEmpty) {
            // Empty line = event boundary
            if (dataLines.nonEmpty) {
              Pull.output1(SSEEvent(eventType, dataLines.reverse.mkString("\n"), eventId)) >>
                go(rest, None, Nil, None)
            } else {
              go(rest, None, Nil, None)
            }
          } else if (line.startsWith(":")) {
            // Comment - ignore
            go(rest, eventType, dataLines, eventId)
          } else if (line.startsWith("event:")) {
            val value = line.drop(6).trim
            go(rest, Some(value), dataLines, eventId)
          } else if (line.startsWith("data:")) {
            val value = line.drop(5).stripPrefix(" ")
            go(rest, eventType, value :: dataLines, eventId)
          } else if (line.startsWith("id:")) {
            val value = line.drop(3).trim
            go(rest, eventType, dataLines, Some(value))
          } else {
            // Unknown field - ignore per SSE spec
            go(rest, eventType, dataLines, eventId)
          }
      }

    in => go(in, None, Nil, None).stream
  }

  /** Parse MessagePack binary format.
    *
    * MessagePack is a binary JSON-compatible format. Each value is decoded
    * and converted to Circe JSON for uniform downstream handling.
    *
    * This parser accumulates bytes across chunk boundaries to handle values
    * that span multiple chunks. It uses MessagePack's self-delimiting format
    * to detect complete values.
    */
  def msgpack: Pipe[IO, Byte, Json] = { (input: Stream[IO, Byte]) =>
    def go(s: Stream[IO, Byte], carry: Array[Byte]): Pull[IO, Json, Unit] =
      s.pull.uncons.flatMap {
        case None =>
          // End of stream - try to parse any remaining bytes
          if (carry.nonEmpty) {
            Pull.eval(IO {
              val unpacker = MessagePack.newDefaultUnpacker(carry)
              val results = scala.collection.mutable.ListBuffer[Json]()
              while (unpacker.hasNext) {
                results += unpackToJson(unpacker)
              }
              unpacker.close()
              results.toList
            }).flatMap(jsons => Pull.output(Chunk.from(jsons)))
          } else {
            Pull.done
          }

        case Some((chunk, rest)) =>
          val bytes = if (carry.isEmpty) chunk.toArray else carry ++ chunk.toArray
          Pull.eval(IO {
            parseChunk(bytes)
          }).flatMap { case (jsons, remaining) =>
            Pull.output(Chunk.from(jsons)) >> go(rest, remaining)
          }
      }

    go(input, Array.empty).stream
  }

  /** Parse as many complete msgpack values from bytes as possible.
    * Returns parsed values and any remaining bytes for the next chunk.
    */
  private def parseChunk(bytes: Array[Byte]): (List[Json], Array[Byte]) = {
    val unpacker = MessagePack.newDefaultUnpacker(bytes)
    val results = scala.collection.mutable.ListBuffer[Json]()
    var lastPosition = 0L
    var incomplete = false

    while (unpacker.hasNext && !incomplete) {
      val positionBefore = unpacker.getTotalReadBytes
      try {
        val json = unpackToJson(unpacker)
        results += json
        lastPosition = unpacker.getTotalReadBytes
      } catch {
        case _: org.msgpack.core.MessageInsufficientBufferException =>
          // Incomplete value - carry remaining bytes to next chunk
          incomplete = true
      }
    }
    unpacker.close()

    val remaining = if (incomplete || lastPosition < bytes.length) {
      bytes.drop(lastPosition.toInt)
    } else {
      Array.empty[Byte]
    }

    (results.toList, remaining)
  }

  /** Convert a single MessagePack value to Circe JSON. */
  private def unpackToJson(unpacker: MessageUnpacker): Json = {
    import org.msgpack.core.MessageFormat

    val format = unpacker.getNextFormat
    format.getValueType match {
      case org.msgpack.value.ValueType.NIL =>
        unpacker.unpackNil()
        Json.Null

      case org.msgpack.value.ValueType.BOOLEAN =>
        Json.fromBoolean(unpacker.unpackBoolean())

      case org.msgpack.value.ValueType.INTEGER =>
        if (format == MessageFormat.UINT64) {
          // Handle unsigned 64-bit as BigInt
          Json.fromBigInt(unpacker.unpackBigInteger())
        } else {
          Json.fromLong(unpacker.unpackLong())
        }

      case org.msgpack.value.ValueType.FLOAT =>
        Json.fromDoubleOrNull(unpacker.unpackDouble())

      case org.msgpack.value.ValueType.STRING =>
        Json.fromString(unpacker.unpackString())

      case org.msgpack.value.ValueType.BINARY =>
        // Encode binary as base64 string
        val len = unpacker.unpackBinaryHeader()
        val bytes = new Array[Byte](len)
        unpacker.readPayload(bytes)
        Json.fromString(java.util.Base64.getEncoder.encodeToString(bytes))

      case org.msgpack.value.ValueType.ARRAY =>
        val size = unpacker.unpackArrayHeader()
        val elements = (0 until size).map(_ => unpackToJson(unpacker)).toVector
        Json.fromValues(elements)

      case org.msgpack.value.ValueType.MAP =>
        val size = unpacker.unpackMapHeader()
        val fields = (0 until size).map { _ =>
          val key = unpacker.unpackString()
          val value = unpackToJson(unpacker)
          (key, value)
        }.toVector
        Json.fromFields(fields)

      case org.msgpack.value.ValueType.EXTENSION =>
        // Extension types: read the payload bytes and skip
        // unpackExtensionTypeHeader returns ExtensionTypeHeader with type and length
        val header = unpacker.unpackExtensionTypeHeader()
        val extBytes = new Array[Byte](header.getLength)
        unpacker.readPayload(extBytes)
        // Return null for extension types (not representable in JSON)
        Json.Null
    }
  }
}

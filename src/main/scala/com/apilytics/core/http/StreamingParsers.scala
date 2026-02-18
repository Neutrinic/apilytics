package com.apilytics.core.http

import cats.effect.IO
import fs2.{Pipe, Pull, Stream}
import io.circe.Json
import io.circe.parser.parse

/** Parsers for streaming response formats (NDJSON, SSE). */
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
}

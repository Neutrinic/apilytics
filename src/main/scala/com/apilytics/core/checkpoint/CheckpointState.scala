package com.apilytics.core.checkpoint

import io.circe.{Json, parser}

/** Represents a saved position in an API's data stream.
  *
  * Serialized to JSON for persistence across queries. Each variant
  * corresponds to a checkpoint mode (cursor, timestamp, offset).
  */
sealed trait CheckpointState extends Serializable {
  def toJson: Json
}

object CheckpointState {

  /** No saved state - start from beginning. */
  case object Empty extends CheckpointState {
    override def toJson: Json = Json.obj("type" -> Json.fromString("empty"))
  }

  /** Cursor-based position from API pagination. */
  final case class CursorValue(value: String) extends CheckpointState {
    override def toJson: Json = Json.obj(
      "type" -> Json.fromString("cursor"),
      "value" -> Json.fromString(value)
    )
  }

  /** Timestamp-based position for time-ordered APIs. */
  final case class TimestampValue(value: String) extends CheckpointState {
    override def toJson: Json = Json.obj(
      "type" -> Json.fromString("timestamp"),
      "value" -> Json.fromString(value)
    )
  }

  /** Numeric offset position. */
  final case class OffsetValue(value: Long) extends CheckpointState {
    override def toJson: Json = Json.obj(
      "type" -> Json.fromString("offset"),
      "value" -> Json.fromLong(value)
    )
  }

  def fromJson(json: Json): Either[String, CheckpointState] = {
    val cursor = json.hcursor
    cursor.get[String]("type").left.map(_.message).flatMap {
      case "empty" => Right(Empty)
      case "cursor" =>
        cursor.get[String]("value").left.map(_.message).map(CursorValue)
      case "timestamp" =>
        cursor.get[String]("value").left.map(_.message).map(TimestampValue)
      case "offset" =>
        cursor.get[Long]("value").left.map(_.message).map(OffsetValue)
      case other => Left(s"Unknown checkpoint state type: $other")
    }
  }

  def fromJsonString(jsonStr: String): Either[String, CheckpointState] =
    parser.parse(jsonStr).left.map(_.message).flatMap(fromJson)
}

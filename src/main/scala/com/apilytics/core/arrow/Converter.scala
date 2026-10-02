package com.apilytics.core.arrow

import com.apilytics.core.schema.SchemaMapper
import io.circe.{Json, JsonNumber}
import io.circe.pointer.Pointer
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}

import scala.jdk.CollectionConverters._

object Converter {

  /** Convert a list of JSON objects to an Arrow VectorSchemaRoot.
    *
    * A value already in its column's type is written as is. One with exactly one reading in
    * that type is converted: a numeric string into a number column, `"true"` or `"false"` into
    * a boolean, a timestamp with a space for `T` or without a zone (read as UTC). Anything else
    * that doesn't fit or doesn't parse becomes NULL rather than failing the read: a fraction in
    * an integer column, an `int32` out of range, an epoch number in a timestamp column (seconds
    * or milliseconds would be a guess), an empty string as a date. `stats` counts both (#308,
    * #309).
    *
    * @param records flat JSON objects (after extracting via data_path)
    * @param schema  the Arrow schema (already flattened by SchemaMapper)
    * @param allocator Arrow buffer allocator
    * @param stats   counts converted and NULLed values, and reports the first NULL per column
    * @return populated VectorSchemaRoot
    */
  def toArrow(
      records: List[Json],
      schema: Schema,
      allocator: BufferAllocator,
      stats: ConversionStats = ConversionStats.ignore
  ): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(schema, allocator)
    root.setRowCount(records.size)

    val fields = schema.getFields.asScala.toList

    fields.foreach { field =>
      val vector = root.getVector(field.getName)
      vector.allocateNew()

      // Check if this is a variant field (entire JSON as string)
      val isVariant = Option(field.getMetadata)
        .flatMap(m => Option(m.get(SchemaMapper.VariantKey)))
        .contains("true")

      if (isVariant) {
        // Write entire record as JSON string
        records.zipWithIndex.foreach { case (record, idx) =>
          writeValue(vector, idx, record, field.getType, field.getName, stats)
        }
      } else {
        // Read the original JSON path from field metadata (set by SchemaMapper)
        val pathParts = Option(field.getMetadata)
          .flatMap(m => Option(m.get(SchemaMapper.JsonPathKey)))
          .map(_.split(",").toList)
          .getOrElse(List(field.getName))

        records.zipWithIndex.foreach { case (record, idx) =>
          val value = navigateJson(record, pathParts)
          writeValue(vector, idx, value, field.getType, field.getName, stats)
        }
      }

      vector.setValueCount(records.size)
    }

    root
  }

  /** Extract records from a JSON response using a JSON Pointer to the data array. */
  def extractRecords(json: Json, dataPath: Option[String]): List[Json] = {
    dataPath match {
      case None =>
        // Assume top-level array
        json.asArray.map(_.toList).getOrElse(List(json))
      case Some(path) =>
        Pointer.parse(path) match {
          case Right(pointer) =>
            pointer.get(json).toOption
              .flatMap(_.asArray)
              .map(_.toList)
              .getOrElse(Nil)
          case Left(_) =>
            throw new IllegalArgumentException(s"Invalid JSON pointer: $path")
        }
    }
  }

  private def navigateJson(json: Json, path: List[String]): Json = {
    path.foldLeft(json) { (current, key) =>
      current.asObject.flatMap(_.apply(key)).getOrElse(Json.Null)
    }
  }

  private def writeValue(
      vector: FieldVector,
      idx: Int,
      value: Json,
      fieldType: ArrowType,
      column: String,
      stats: ConversionStats
  ): Unit = {
    if (value.isNull) {
      setNull(vector, idx)
      return
    }

    /** Write `read`'s value, counting a conversion; with none, write NULL and count that. */
    def write[A](read: Option[Read[A]], typeName: String)(set: A => Unit): Unit =
      read match {
        case Some(Read(a, converted)) =>
          set(a)
          if (converted) stats.recordConverted()
        case None =>
          setNull(vector, idx)
          stats.recordNulled(column, value, typeName)
      }

    (vector, fieldType) match {
      case (v: VarCharVector, _: ArrowType.Utf8) =>
        // For VARIANT fields (deep objects/arrays), serialize as JSON string
        val str = value.asString.getOrElse(value.noSpaces)
        val bytes = str.getBytes("UTF-8")
        v.setSafe(idx, bytes, 0, bytes.length)

      case (v: IntVector, i: ArrowType.Int) if i.getBitWidth == 32 =>
        write(readLong(value).filter(r => r.value.isValidInt), "int")(r => v.setSafe(idx, r.toInt))

      case (v: BigIntVector, i: ArrowType.Int) if i.getBitWidth == 64 =>
        write(readLong(value), "bigint")(v.setSafe(idx, _))

      case (v: Float8Vector, _: ArrowType.FloatingPoint) =>
        write(readDouble(value), "double")(v.setSafe(idx, _))

      case (v: BitVector, _: ArrowType.Bool) =>
        write(readBoolean(value), "boolean")(b => v.setSafe(idx, if (b) 1 else 0))

      case (v: DateDayVector, _: ArrowType.Date) =>
        write(value.asString.flatMap(readDate), "date")(d => v.setSafe(idx, d.toEpochDay.toInt))

      case (v: TimeStampMicroTZVector, _: ArrowType.Timestamp) =>
        write(value.asString.flatMap(readTimestamp), "timestamp") { instant =>
          v.setSafe(idx, instant.getEpochSecond * 1_000_000 + instant.getNano / 1000)
        }

      case _ =>
        setNull(vector, idx)
    }
  }

  /** A value read into a column's type, and whether that took a conversion. */
  private final case class Read[A](value: A, converted: Boolean)

  private val IntegerText = "[+-]?[0-9]+".r
  // A decimal number as JSON writes one, without JSON's ban on a leading `+` or zeros.
  private val DecimalText = """[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?""".r

  /** A JSON integer, or a string holding one. `3.5` and `"3.0"` aren't integers. */
  private def readLong(value: Json): Option[Read[Long]] =
    value.asNumber match {
      case Some(n) => n.toLong.map(Read(_, converted = false))
      case None =>
        value.asString.collect { case s @ IntegerText() => s }
          .flatMap(_.toLongOption).map(Read(_, converted = true))
    }

  private def readDouble(value: Json): Option[Read[Double]] =
    value.asNumber match {
      case Some(n) => Some(Read(n.toDouble, converted = false))
      case None =>
        value.asString.collect { case s @ DecimalText() => s }
          .flatMap(_.toDoubleOption).map(Read(_, converted = true))
    }

  private def readBoolean(value: Json): Option[Read[Boolean]] =
    value.asBoolean match {
      case Some(b) => Some(Read(b, converted = false))
      case None =>
        value.asString.map(_.toLowerCase(java.util.Locale.ROOT)).collect {
          case "true"  => Read(true, converted = true)
          case "false" => Read(false, converted = true)
        }
    }

  private def readDate(s: String): Option[Read[java.time.LocalDate]] =
    try Some(Read(java.time.LocalDate.parse(s), converted = false))
    catch { case _: java.time.DateTimeException => None }

  /** An ISO-8601 instant, read as the timestamp column it came from would read it.
    * Checkpoints and stream bounds compare these values, so they parse them the same way.
    */
  private[apilytics] def parseTimestamp(s: String): Option[java.time.Instant] =
    readTimestamp(s).map(_.value)

  /** An ISO-8601 instant. Also, as conversions: a space in place of `T`, and no zone, read
    * as UTC.
    */
  private def readTimestamp(s: String): Option[Read[java.time.Instant]] = {
    import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset}
    def attempt[A](f: => A): Option[A] =
      try Some(f) catch { case _: java.time.DateTimeException => None }

    attempt(Instant.parse(s)).map(Read(_, converted = false))
      .orElse(attempt(OffsetDateTime.parse(s).toInstant).map(Read(_, converted = false)))
      .orElse {
        val t = if (s.length > 10 && s.charAt(10) == ' ') s.updated(10, 'T') else s
        attempt(OffsetDateTime.parse(t).toInstant)
          .orElse(attempt(LocalDateTime.parse(t).toInstant(ZoneOffset.UTC)))
          .map(Read(_, converted = true))
      }
  }

  private def setNull(vector: FieldVector, idx: Int): Unit = {
    vector match {
      case v: VarCharVector        => v.setNull(idx)
      case v: IntVector            => v.setNull(idx)
      case v: BigIntVector         => v.setNull(idx)
      case v: Float8Vector         => v.setNull(idx)
      case v: BitVector            => v.setNull(idx)
      case v: DateDayVector        => v.setNull(idx)
      case v: TimeStampMicroTZVector => v.setNull(idx)
      case _                       => () // best effort
    }
  }
}

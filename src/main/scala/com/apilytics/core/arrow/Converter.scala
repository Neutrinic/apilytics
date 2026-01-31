package com.apilytics.core.arrow

import io.circe.{Json, JsonNumber}
import io.circe.pointer.Pointer
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}

import scala.jdk.CollectionConverters._

object Converter {

  /** Convert a list of JSON objects to an Arrow VectorSchemaRoot.
    *
    * @param records flat JSON objects (after extracting via data_path)
    * @param schema  the Arrow schema (already flattened by SchemaMapper)
    * @param allocator Arrow buffer allocator
    * @return populated VectorSchemaRoot
    */
  def toArrow(records: List[Json], schema: Schema, allocator: BufferAllocator): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(schema, allocator)
    root.setRowCount(records.size)

    val fields = schema.getFields.asScala.toList

    fields.foreach { field =>
      val vector = root.getVector(field.getName)
      vector.allocateNew()

      // Field name may be flattened (e.g., "address_city") — split back to navigate JSON
      val pathParts = field.getName.split("_").toList

      records.zipWithIndex.foreach { case (record, idx) =>
        val value = navigateJson(record, pathParts)
        writeValue(vector, idx, value, field.getType)
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

  private def writeValue(vector: FieldVector, idx: Int, value: Json, fieldType: ArrowType): Unit = {
    if (value.isNull) {
      setNull(vector, idx)
      return
    }

    (vector, fieldType) match {
      case (v: VarCharVector, _: ArrowType.Utf8) =>
        // For VARIANT fields (deep objects/arrays), serialize as JSON string
        val str = value.asString.getOrElse(value.noSpaces)
        val bytes = str.getBytes("UTF-8")
        v.setSafe(idx, bytes, 0, bytes.length)

      case (v: IntVector, _: ArrowType.Int) if fieldType.asInstanceOf[ArrowType.Int].getBitWidth == 32 =>
        value.asNumber.flatMap(_.toInt) match {
          case Some(i) => v.setSafe(idx, i)
          case None    => setNull(vector, idx)
        }

      case (v: BigIntVector, _: ArrowType.Int) if fieldType.asInstanceOf[ArrowType.Int].getBitWidth == 64 =>
        value.asNumber.flatMap(_.toLong) match {
          case Some(l) => v.setSafe(idx, l)
          case None    => setNull(vector, idx)
        }

      case (v: Float8Vector, _: ArrowType.FloatingPoint) =>
        value.asNumber.map(_.toDouble) match {
          case Some(d) => v.setSafe(idx, d)
          case None    => setNull(vector, idx)
        }

      case (v: BitVector, _: ArrowType.Bool) =>
        value.asBoolean match {
          case Some(b) => v.setSafe(idx, if (b) 1 else 0)
          case None    => setNull(vector, idx)
        }

      case (v: DateDayVector, _: ArrowType.Date) =>
        value.asString.map { s =>
          val days = java.time.LocalDate.parse(s).toEpochDay.toInt
          v.setSafe(idx, days)
        }.getOrElse(setNull(vector, idx))

      case (v: TimeStampMicroTZVector, _: ArrowType.Timestamp) =>
        value.asString.map { s =>
          val instant = java.time.Instant.parse(s)
          val micros = instant.getEpochSecond * 1_000_000 + instant.getNano / 1000
          v.setSafe(idx, micros)
        }.getOrElse(setNull(vector, idx))

      case _ =>
        setNull(vector, idx)
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

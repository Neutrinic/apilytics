package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.http.{Client, Paginator}
import com.apilytics.core.schema.SchemaMapper
import io.circe.Json
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String
import org.http4s.Uri

import scala.jdk.CollectionConverters._

/** Partition reader that explodes an array field from API responses.
  * Each row contains parent scalar fields plus one array element.
  */
class ExplodedArrayPartitionReader(partition: ExplodedArrayInputPartition) extends PartitionReader[InternalRow] {

  private val allocator = new RootAllocator()
  private val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  private val rows: Iterator[InternalRow] = fetchExplodedRows()
  private var currentRow: InternalRow = _

  override def next(): Boolean = {
    if (rows.hasNext) {
      currentRow = rows.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    allocator.close()
  }

  private def fetchExplodedRows(): Iterator[InternalRow] = {
    val baseUri = Uri.unsafeFromString(partition.baseUrl + partition.endpoint.path)
    val dataPath = partition.tableConfig.flatMap(_.dataPath)

    val program: IO[List[InternalRow]] = Client
      .resource(partition.sourceConfig.http, partition.sourceConfig.auth)
      .use { client =>
        Paginator
          .pages(client, baseUri, Map.empty, partition.sourceConfig.pagination, None)
          .flatMap { pageJson =>
            val records = Converter.extractRecords(pageJson, dataPath)
            val exploded = records.flatMap(explodeRecord)
            if (exploded.isEmpty) fs2.Stream.empty
            else {
              val root = Converter.toArrow(exploded, arrowSchema, allocator)
              val rows = try {
                arrowToInternalRows(root)
              } finally {
                root.close()
              }
              fs2.Stream.emits(rows)
            }
          }
          .compile
          .toList
      }

    program.unsafeRunSync().iterator
  }

  /** Explode a single record by its array field.
    * For each element in the array, create a new JSON object with:
    * - All parent scalar fields (excluding the array field)
    * - The array element (as the array field name for primitives, or flattened for objects)
    */
  private def explodeRecord(record: Json): List[Json] = {
    val obj = record.asObject.getOrElse(return Nil)

    // Navigate to the array field using the JSON path
    val arrayJson = partition.arrayJsonPath.foldLeft(record) { (current, key) =>
      current.asObject.flatMap(_.apply(key)).getOrElse(Json.Null)
    }

    val arrayElements = arrayJson.asArray.getOrElse(return Nil)
    if (arrayElements.isEmpty) return Nil

    // Parent fields: all fields except the array field
    val parentFields = obj.toMap - partition.arrayFieldName

    arrayElements.toList.map { element =>
      // Create exploded record: parent fields + array element
      val elementFields = element.asObject match {
        case Some(elemObj) =>
          // For object elements, the fields will be prefixed by arrayFieldName during schema mapping
          Map(partition.arrayFieldName -> element)
        case None =>
          // For primitive elements, just use the array field name
          Map(partition.arrayFieldName -> element)
      }

      Json.fromFields(parentFields ++ elementFields)
    }
  }

  private def arrowToInternalRows(root: org.apache.arrow.vector.VectorSchemaRoot): List[InternalRow] = {
    val fieldCount = root.getFieldVectors.size()
    val rowCount = root.getRowCount

    (0 until rowCount).map { rowIdx =>
      val values = new Array[Any](fieldCount)
      root.getFieldVectors.asScala.zipWithIndex.foreach { case (vector, colIdx) =>
        values(colIdx) = if (vector.isNull(rowIdx)) {
          null
        } else {
          vector match {
            case v: org.apache.arrow.vector.VarCharVector =>
              UTF8String.fromBytes(v.get(rowIdx))
            case v: org.apache.arrow.vector.IntVector =>
              v.get(rowIdx)
            case v: org.apache.arrow.vector.BigIntVector =>
              v.get(rowIdx)
            case v: org.apache.arrow.vector.Float8Vector =>
              v.get(rowIdx)
            case v: org.apache.arrow.vector.BitVector =>
              v.get(rowIdx) == 1
            case v: org.apache.arrow.vector.DateDayVector =>
              v.get(rowIdx)
            case v: org.apache.arrow.vector.TimeStampMicroTZVector =>
              v.get(rowIdx)
            case _ => null
          }
        }
      }
      new GenericInternalRow(values)
    }.toList
  }
}

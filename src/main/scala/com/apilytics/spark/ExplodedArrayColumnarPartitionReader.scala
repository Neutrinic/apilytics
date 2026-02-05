package com.apilytics.spark

import cats.effect.{IO, Resource}
import com.apilytics.core.arrow.Converter
import com.apilytics.core.http.{Client, Paginator}
import io.circe.Json
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.http4s.Uri

/** Zero-copy columnar reader for exploded array views.
  * Each API record is exploded by the array field, then lazily streamed as Arrow batches.
  * Memory scales with batch size, not total partition size.
  */
class ExplodedArrayColumnarPartitionReader(partition: ExplodedArrayInputPartition)
    extends LazyColumnarReader {

  override protected val allocator: RootAllocator = new RootAllocator()
  override protected val arrowSchema: ArrowSchema = ArrowSchema.fromJSON(partition.arrowSchemaJson)

  override protected def clientResource: Resource[IO, Client.RestClient] =
    Client.resource(partition.sourceConfig.http, partition.sourceConfig.auth)

  override protected def buildStream(
      client: Client.RestClient
  ): fs2.Stream[IO, (ColumnarBatch, VectorSchemaRoot)] = {
    val baseUri = Uri.unsafeFromString(partition.baseUrl + partition.endpoint.path)
    val dataPath = partition.tableConfig.flatMap(_.dataPath)
    val batchSize = partition.sourceConfig.schema.arrowBatchSize

    Paginator
      .pages(client, baseUri, partition.pushedParams, partition.sourceConfig.pagination, partition.pushedLimit)
      .flatMap { pageJson =>
        val records = Converter.extractRecords(pageJson, dataPath)
        val exploded = records.flatMap(explodeRecord)
        if (exploded.isEmpty) fs2.Stream.empty
        else {
          val chunks = exploded.grouped(batchSize).toList
          fs2.Stream.emits(chunks.map { chunk =>
            val root = Converter.toArrow(chunk, arrowSchema, allocator)
            (arrowToBatch(root), root)
          })
        }
      }
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
}

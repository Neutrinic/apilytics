package com.apilytics.spark

import io.circe.Json

/** Shared operations for exploded array views.
  * Used by both columnar and row-based partition readers.
  */
object ExplodedArrayOps {

  /** Explode a single record by its array field (INNER semantics).
    *
    * For each element in the array, creates a new JSON object with:
    * - All parent scalar fields (excluding the array field)
    * - The array element (keyed by arrayFieldName)
    *
    * Empty or null arrays return Nil (parent row is dropped).
    * This is INNER explode semantics — see issue #41 for OUTER support.
    *
    * @param record         The source JSON record containing the array
    * @param arrayFieldName The name of the array field to explode
    * @param arrayJsonPath  JSON path to navigate to the array (supports nested arrays)
    * @return List of exploded JSON objects, one per array element
    */
  def explodeRecord(record: Json, arrayFieldName: String, arrayJsonPath: List[String]): List[Json] = {
    val obj = record.asObject.getOrElse(return Nil)

    // Navigate to the array field using the JSON path
    val arrayJson = arrayJsonPath.foldLeft(record) { (current, key) =>
      current.asObject.flatMap(_.apply(key)).getOrElse(Json.Null)
    }

    val arrayElements = arrayJson.asArray.getOrElse(return Nil)
    if (arrayElements.isEmpty) return Nil

    // Parent fields: all fields except the array field
    val parentFields = obj.toMap - arrayFieldName

    arrayElements.toList.map { element =>
      // Create exploded record: parent fields + array element
      // Both object and primitive elements are keyed by arrayFieldName
      Json.fromFields(parentFields + (arrayFieldName -> element))
    }
  }
}

package com.apilytics.spark

import io.circe.Json

/** Shared operations for exploded array views.
  * Used by both columnar and row-based partition readers.
  */
object ExplodedArrayOps {

  /** Explode a single record by its array field.
    *
    * For each element in the array, creates a new JSON object with:
    * - All parent scalar fields (excluding the array field)
    * - The array element (keyed by arrayFieldName)
    *
    * @param record         The source JSON record containing the array
    * @param arrayFieldName The name of the array field to explode
    * @param arrayJsonPath  JSON path to navigate to the array (supports nested arrays)
    * @param outer          When true, emit one row with null element for empty/null arrays (OUTER).
    *                       When false, empty arrays produce no rows (INNER).
    * @return List of exploded JSON objects, one per array element (or one null row if OUTER)
    */
  def explodeRecord(
      record: Json,
      arrayFieldName: String,
      arrayJsonPath: List[String],
      outer: Boolean = false
  ): List[Json] = {
    val obj = record.asObject.getOrElse(return Nil)

    // Navigate to the array field using the JSON path
    val arrayJson = arrayJsonPath.foldLeft(record) { (current, key) =>
      current.asObject.flatMap(_.apply(key)).getOrElse(Json.Null)
    }

    // Parent fields: all fields except the array field
    val parentFields = obj.toMap - arrayFieldName

    val arrayElements = arrayJson.asArray
    arrayElements match {
      case Some(elements) if elements.nonEmpty =>
        elements.toList.map { element =>
          Json.fromFields(parentFields + (arrayFieldName -> element))
        }
      case _ =>
        // Empty or null array
        if (outer) {
          // OUTER: emit one row with null element
          List(Json.fromFields(parentFields + (arrayFieldName -> Json.Null)))
        } else {
          // INNER: drop the row
          Nil
        }
    }
  }
}

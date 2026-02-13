package com.apilytics.spark

import io.circe.Json

/** Shared utilities for parent-child join operations.
  *
  * Used by both ParentChildColumnarPartitionReader and ParentChildPartitionReader
  * to avoid code duplication.
  */
object ParentChildUtils {

  /** Convert JSON value to string representation for use in URLs/query params.
    * Handles strings, numbers, and booleans.
    */
  def jsonToString(json: Json): Option[String] = {
    json.asString
      .orElse(json.asNumber.map(_.toString))
      .orElse(json.asBoolean.map(_.toString))
  }

  /** Enrich a child record with the parent key column.
    * Preserves the original JSON type of the parent key.
    */
  def enrichChildRecord(
      childRecord: Json,
      parentKeyJson: Json,
      parentKeyColumn: String
  ): Json = {
    childRecord.asObject match {
      case Some(obj) =>
        Json.fromFields(
          (parentKeyColumn -> parentKeyJson) +: obj.toList
        )
      case None =>
        // Non-object child record - wrap it
        Json.obj(
          parentKeyColumn -> parentKeyJson,
          "value" -> childRecord
        )
    }
  }

  /** Find which parent key corresponds to this child record.
    *
    * For batch joins, the child record should contain a field that references the parent.
    * Tries childKeyField first if specified, then common naming patterns.
    *
    * @param childRecord The child record from the API response
    * @param parentKeysMap Map of parent key strings to their original JSON values
    * @param parentKeyField The parent key field name (e.g., "id")
    * @param childKeyField Optional explicit child field name (e.g., "proj_id")
    * @return The parent key JSON value if a match is found
    */
  def findParentKeyForChild(
      childRecord: Json,
      parentKeysMap: Map[String, Json],
      parentKeyField: String,
      childKeyField: Option[String] = None
  ): Option[Json] = {
    // Use explicit childKeyField if configured, otherwise derive from parentKeyField.
    // Note: We intentionally don't try "id" as a fallback because child records
    // almost always have their own "id" field, which would cause silent wrong joins.
    val childKeyCandidates = childKeyField match {
      case Some(explicitField) =>
        // User specified exact field name in child records
        List(explicitField)
      case None =>
        // Try common naming patterns: parentKeyField, parentKeyField_id
        List(parentKeyField, s"${parentKeyField}_id")
    }

    childRecord.asObject.flatMap { obj =>
      childKeyCandidates.view
        .flatMap(obj.apply)
        .flatMap { childRef =>
          val refString = jsonToString(childRef)
          refString.flatMap(parentKeysMap.get)
        }
        .headOption
    }
  }
}

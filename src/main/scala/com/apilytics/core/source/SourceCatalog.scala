package com.apilytics.core.source

import com.apilytics.core.schema.SourceSchema

/** One readable table: what it is called, what its records look like, and how to read it.
  *
  * The schema here is the *effective record* schema — what a single record looks like
  * after the source has done whatever unwrapping it needs (for REST, resolving
  * `data-path` into a response body, or unwrapping a top-level array). Callers get the
  * shape of a row, not the shape of a response.
  */
final case class TableSpec(
    name: String,
    schema: SourceSchema.ObjectType,
    handle: SourceHandle
)

/** Discovery half of the protocol-neutral interface (#191).
  *
  * Answers "what tables are there" and "what is this one", so the Spark layer never has
  * to know that REST tables come from an OpenAPI spec, or that resolving one involves
  * path templates, operation ids and config overrides.
  */
trait SourceCatalog {

  /** Tables this source exposes, before any Spark-side expansion such as array views. */
  def tableNames: Seq[String]

  /** Resolve one table, or `None` if this source does not have it. */
  def table(name: String): Option[TableSpec]
}

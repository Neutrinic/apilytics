package com.apilytics.spark

import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

class ExplodedArrayScanBuilder(
    table: ExplodedArrayTable,
    arrowSchema: ArrowSchema
) extends ScanBuilder
    with SupportsPushDownRequiredColumns {

  private var prunedSchema: Option[StructType] = None

  override def pruneColumns(requiredSchema: StructType): Unit = {
    prunedSchema = Some(requiredSchema)
  }

  override def build(): Scan = {
    val finalArrowSchema = prunedSchema match {
      case Some(required) => ArrowSchemaConverter.pruneSchema(arrowSchema, required)
      case None           => arrowSchema
    }
    new ExplodedArrayScan(table, finalArrowSchema, prunedSchema)
  }
}

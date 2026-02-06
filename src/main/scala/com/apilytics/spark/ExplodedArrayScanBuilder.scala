package com.apilytics.spark

import com.apilytics.core.config.FilterConfig
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownLimit, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

class ExplodedArrayScanBuilder(
    table: ExplodedArrayTable,
    arrowSchema: ArrowSchema
) extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownLimit
    with SupportsPushDownRequiredColumns
    with FilterPushdown {

  private var prunedSchema: Option[StructType] = None

  override protected val filterConfigs: List[FilterConfig] =
    table.tableConfig.map(_.filters).getOrElse(Nil)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    prunedSchema = Some(requiredSchema)
  }

  override def build(): Scan = {
    val finalArrowSchema = prunedSchema match {
      case Some(required) => ArrowSchemaConverter.pruneSchema(arrowSchema, required)
      case None           => arrowSchema
    }
    new ExplodedArrayScan(table, finalArrowSchema, prunedSchema, pushedParams, pushedLimit)
  }
}

package com.apilytics.spark

import com.apilytics.core.config.{FilterConfig, ReservedParams}
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownLimit, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

class ParentChildScanBuilder(
    table: ParentChildTable,
    arrowSchema: ArrowSchema
) extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownLimit
    with SupportsPushDownRequiredColumns
    with FilterPushdown {

  private var prunedSchema: Option[StructType] = None

  override protected val filterConfigs: List[FilterConfig] =
    table.tableConfig.filters

  // Filters go on the child requests, which use this table's pagination and batch-param.
  override protected def reservedParams: Map[String, String] =
    ReservedParams.forFilters(Some(table.tableConfig), table.sourceConfig)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    prunedSchema = Some(requiredSchema)
  }

  override def build(): Scan = {
    val finalArrowSchema = prunedSchema match {
      case Some(required) => ArrowSchemaConverter.pruneSchema(arrowSchema, required)
      case None           => arrowSchema
    }
    new ParentChildScan(table, finalArrowSchema, prunedSchema, pushedParams, pushedLimit)
  }
}

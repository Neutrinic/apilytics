package com.apilytics.spark

import com.apilytics.core.config.{CountConfig, FilterConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, CountStar}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownLimit, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

class RESTScanBuilder(table: RESTTable) extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownLimit
    with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates
    with FilterPushdown
    with Logging {

  private var prunedSchema: Option[StructType] = None
  private var pushedAggregation: Option[Aggregation] = None

  override protected val filterConfigs: List[FilterConfig] =
    table.tableConfig.map(_.filters).getOrElse(Nil)

  private val countConfig: Option[CountConfig] =
    table.tableConfig.flatMap(_.count)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    prunedSchema = Some(requiredSchema)
  }

  /** Push aggregation to the API if supported.
    *
    * We only support COUNT(*) when a count config is provided.
    * The API must have either a dedicated count endpoint or a count query param.
    */
  override def pushAggregation(aggregation: Aggregation): Boolean = {
    // Only support if count config is present
    if (countConfig.isEmpty) {
      logDebug("Aggregation pushdown rejected: no count config")
      return false
    }

    // Only support COUNT(*) without GROUP BY
    val aggs = aggregation.aggregateExpressions()
    val groups = aggregation.groupByExpressions()

    if (groups.nonEmpty) {
      logDebug(s"Aggregation pushdown rejected: GROUP BY not supported (${groups.length} grouping columns)")
      return false
    }

    if (aggs.length != 1) {
      logDebug(s"Aggregation pushdown rejected: only single COUNT(*) supported (got ${aggs.length} aggregates)")
      return false
    }

    aggs(0) match {
      case _: CountStar =>
        logInfo("Aggregation pushed to API: COUNT(*)")
        pushedAggregation = Some(aggregation)
        true
      case other =>
        logDebug(s"Aggregation pushdown rejected: unsupported aggregate function ${other.getClass.getSimpleName}")
        false
    }
  }

  /** Return true if we can fully compute the aggregation without Spark doing any post-aggregation.
    *
    * For COUNT(*) without GROUP BY, we return the exact count from the API, so Spark
    * doesn't need to do any additional aggregation.
    */
  override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
    // Complete pushdown only for COUNT(*) without GROUP BY
    val groups = aggregation.groupByExpressions()
    val aggs = aggregation.aggregateExpressions()

    groups.isEmpty && aggs.length == 1 && aggs(0).isInstanceOf[CountStar] && countConfig.isDefined
  }

  override def build(): Scan = {
    val finalArrowSchema = prunedSchema match {
      case Some(required) => ArrowSchemaConverter.pruneSchema(table.arrowSchema, required)
      case None           => table.arrowSchema
    }
    new RESTScan(table, finalArrowSchema, prunedSchema, pushedParams, pushedLimit, pushedAggregation)
  }
}

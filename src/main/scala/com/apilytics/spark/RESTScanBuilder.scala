package com.apilytics.spark

import com.apilytics.core.config.{AggregationConfig, AggregationFunction, CountConfig, FilterConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate._
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
  /** Resolved aggregation configs for the pushed aggregation. */
  private var resolvedAggConfigs: List[AggregationConfig] = Nil

  override protected val filterConfigs: List[FilterConfig] =
    table.tableConfig.map(_.filters).getOrElse(Nil)

  /** Legacy count config for backwards compatibility. */
  private val countConfig: Option[CountConfig] =
    table.tableConfig.flatMap(_.count)

  /** New aggregation configs. */
  private val aggregationConfigs: Map[String, AggregationConfig] =
    table.tableConfig.map(_.aggregations).getOrElse(Map.empty)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    prunedSchema = Some(requiredSchema)
  }

  /** Push aggregation to the API if supported.
    *
    * Supports:
    * - COUNT(*) via legacy count config or new aggregations
    * - SUM, AVG, MIN, MAX when configured in aggregations
    * - Custom aggregations matched by name
    */
  override def pushAggregation(aggregation: Aggregation): Boolean = {
    val aggs = aggregation.aggregateExpressions()
    val groups = aggregation.groupByExpressions()

    // GROUP BY not supported
    if (groups.nonEmpty) {
      logDebug(s"Aggregation pushdown rejected: GROUP BY not supported (${groups.length} grouping columns)")
      return false
    }

    // Try to match all aggregate expressions to configs
    val matched = aggs.flatMap(matchAggregateToConfig)

    if (matched.length != aggs.length) {
      logDebug(s"Aggregation pushdown rejected: could only match ${matched.length} of ${aggs.length} aggregates")
      return false
    }

    // All matched - accept the pushdown
    val descriptions = aggs.map(describeAggregate).mkString(", ")
    logInfo(s"Aggregations pushed to API: $descriptions")
    pushedAggregation = Some(aggregation)
    resolvedAggConfigs = matched.toList
    true
  }

  /** Match a Spark aggregate expression to an AggregationConfig. */
  private def matchAggregateToConfig(agg: AggregateFunc): Option[AggregationConfig] = {
    agg match {
      case _: CountStar =>
        // Try new config first, then legacy
        findAggConfig(AggregationFunction.Count, None)
          .orElse(countConfig.map(legacyCountToAggConfig))

      case c: Count =>
        val col = extractColumnName(c.column())
        findAggConfig(AggregationFunction.Count, col)

      case s: Sum =>
        val col = extractColumnName(s.column())
        findAggConfig(AggregationFunction.Sum, col)

      case a: Avg =>
        val col = extractColumnName(a.column())
        findAggConfig(AggregationFunction.Avg, col)

      case m: Min =>
        val col = extractColumnName(m.column())
        findAggConfig(AggregationFunction.Min, col)

      case m: Max =>
        val col = extractColumnName(m.column())
        findAggConfig(AggregationFunction.Max, col)

      case g: GeneralAggregateFunc =>
        // Custom aggregation - match by name
        findCustomAggConfig(g.name())

      case _ =>
        None
    }
  }

  /** Find an aggregation config matching function and optional column. */
  private def findAggConfig(
      function: AggregationFunction,
      column: Option[String]
  ): Option[AggregationConfig] = {
    aggregationConfigs.values.find { config =>
      config.function == function && (column.isEmpty || config.column == column)
    }
  }

  /** Find a custom aggregation config by name. */
  private def findCustomAggConfig(name: String): Option[AggregationConfig] = {
    aggregationConfigs.values.find {
      case AggregationConfig(AggregationFunction.Custom(n), _, _, _, _) =>
        n.equalsIgnoreCase(name)
      case _ => false
    }
  }

  /** Convert legacy CountConfig to AggregationConfig. */
  private def legacyCountToAggConfig(cc: CountConfig): AggregationConfig = {
    AggregationConfig(
      function = AggregationFunction.Count,
      column = None,
      endpoint = cc.endpoint.getOrElse(table.tableConfig.map(_.endpoint).getOrElse("")),
      responsePath = cc.responsePath,
      params = cc.param.map(p => Map(p -> cc.paramValue)).getOrElse(Map.empty)
    )
  }

  /** Extract column name from Spark expression. */
  private def extractColumnName(expr: org.apache.spark.sql.connector.expressions.Expression): Option[String] = {
    expr match {
      case ref: NamedReference =>
        val names = ref.fieldNames()
        if (names.nonEmpty) Some(names.mkString(".")) else None
      case _ => None
    }
  }

  /** Describe an aggregate for logging. */
  private def describeAggregate(agg: AggregateFunc): String = {
    agg match {
      case _: CountStar => "COUNT(*)"
      case c: Count => s"COUNT(${extractColumnName(c.column()).getOrElse("?")})"
      case s: Sum => s"SUM(${extractColumnName(s.column()).getOrElse("?")})"
      case a: Avg => s"AVG(${extractColumnName(a.column()).getOrElse("?")})"
      case m: Min => s"MIN(${extractColumnName(m.column()).getOrElse("?")})"
      case m: Max => s"MAX(${extractColumnName(m.column()).getOrElse("?")})"
      case g: GeneralAggregateFunc => s"${g.name()}(...)"
      case other => other.getClass.getSimpleName
    }
  }

  /** Return true if we can fully compute the aggregation without Spark doing any post-aggregation.
    *
    * For aggregations without GROUP BY where all functions are pushed, we return exact values
    * from the API, so Spark doesn't need to do any additional aggregation.
    */
  override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
    // pushAggregation already validated and stored resolved configs
    // Complete pushdown if we matched all aggregates (no GROUP BY check needed - already done)
    resolvedAggConfigs.length == aggregation.aggregateExpressions().length
  }

  override def build(): Scan = {
    val finalArrowSchema = prunedSchema match {
      case Some(required) => ArrowSchemaConverter.pruneSchema(table.arrowSchema, required)
      case None           => table.arrowSchema
    }
    new RESTScan(
      table,
      finalArrowSchema,
      prunedSchema,
      pushedParams,
      pushedLimit,
      pushedAggregation,
      resolvedAggConfigs
    )
  }
}

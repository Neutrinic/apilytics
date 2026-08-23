package com.apilytics.spark

import com.apilytics.core.config.{AggregationConfig, AggregationFunction, AggregationResultType, CountConfig, FilterConfig}
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

    // Push only the aggregates whose result type is unambiguous, because the scan has to
    // agree with the type Spark has already given the aggregate expression — advertising a
    // different one crashes the query during optimization (#212).
    //
    // COUNT is always whole, AVG always fractional, and SUM promotes integral columns to
    // long. MIN/MAX instead preserve the column type exactly (MAX over an int column is an
    // int, not a bigint) across every orderable type including dates and decimals, and a
    // custom aggregate's type is decided inside Spark with no way to read it. Reproducing
    // either faithfully is guesswork, so they are declined.
    //
    // Declining costs a full read but is never wrong: Spark aggregates the scanned rows.
    val undecidable = matched.filter { c =>
      c.function match {
        case AggregationFunction.Min | AggregationFunction.Max => true
        case AggregationFunction.Custom(_)                     => true
        case _                                                 => false
      }
    }
    if (undecidable.nonEmpty) {
      logInfo(
        s"Aggregation pushdown declined for ${undecidable.map(_.function).mkString(", ")}: " +
          "result type is not decidable at plan time (#213). Spark will aggregate these itself."
      )
      return false
    }

    // Fix each aggregate's result type now, at plan time. Spark needs the scan's schema
    // before any data arrives, and the reader coerces values to what is decided here —
    // otherwise an API answering `42` on one call and `42.0` on the next would not match
    // the advertised schema, which is what crashed COUNT in #212.
    val typed = matched.map(cfg => cfg.copy(resultType = Some(resolveResultType(cfg))))

    val descriptions = aggs.map(describeAggregate).mkString(", ")
    logInfo(s"Aggregations pushed to API: $descriptions")
    pushedAggregation = Some(aggregation)
    resolvedAggConfigs = typed.toList
    true
  }

  /** Decide an aggregate's result type, mirroring Spark's own aggregate typing.
    *
    * This is not a free choice: Spark has already typed the aggregate expression, so the
    * scan has to agree with it. COUNT is whole, AVG is fractional, and SUM/MIN/MAX take
    * the type of the column being aggregated — the same rules Spark applies.
    */
  private def resolveResultType(cfg: AggregationConfig): AggregationResultType =
    cfg.resultType.getOrElse {
      import org.apache.spark.sql.types._
      cfg.function match {
        case AggregationFunction.Count => AggregationResultType.Long
        case AggregationFunction.Avg   => AggregationResultType.Double
        case _ =>
          cfg.column
            .flatMap(c => table.schema().fields.find(_.name == c))
            .map(_.dataType match {
              case _: IntegerType | _: LongType | _: ShortType | _: ByteType => AggregationResultType.Long
              case _: StringType                                            => AggregationResultType.String
              case _: BooleanType                                           => AggregationResultType.Boolean
              case _                                                        => AggregationResultType.Double
            })
            .getOrElse(AggregationResultType.Double)
      }
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
      case AggregationConfig(AggregationFunction.Custom(n), _, _, _, _, _) =>
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

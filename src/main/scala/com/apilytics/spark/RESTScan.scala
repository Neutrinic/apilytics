package com.apilytics.spark

import com.apilytics.core.config.{AggregationConfig, PartitionConfig}
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.types.StructType

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

class RESTScan(
    table: RESTTable,
    arrowSchema: ArrowSchema,
    prunedSchema: Option[StructType],
    pushedParams: Map[String, String],
    pushedLimit: Option[Int],
    pushedAggregation: Option[Aggregation] = None,
    resolvedAggConfigs: List[AggregationConfig] = Nil
) extends Scan with Batch with SupportsReportStatistics with Logging {

  /** Schema of what this scan actually produces.
    *
    * Once an aggregation is pushed, that is one row of aggregate values — not table
    * rows — and `SupportsPushDownAggregates` requires the schema to say so. Returning
    * the table schema regardless made Spark assert on the column count and fail the
    * query during optimization, so COUNT pushdown never worked (#212).
    */
  override def readSchema(): StructType =
    if (pushedAggregation.isDefined) aggregateSchema
    else prunedSchema.getOrElse(table.schema())

  /** One field per pushed aggregate, in the order the reader emits them.
    *
    * Only COUNT reaches here — `RESTScanBuilder.pushAggregation` declines everything else,
    * because their result types are decided by the JSON that comes back rather than at plan
    * time, and Spark needs the schema up front (#213). COUNT is always a whole number.
    */
  private def aggregateSchema: StructType = {
    import org.apache.spark.sql.types._

    // Legacy count config produces one COUNT column with no resolved config.
    val count = math.max(resolvedAggConfigs.size, 1)
    StructType((0 until count).map(i => StructField(s"count_$i", LongType, nullable = true)))
  }

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // If aggregation is pushed, return an aggregation partition
    pushedAggregation match {
      case Some(agg) =>
        planAggregationPartition(agg)
      case None =>
        planDataPartitions()
    }
  }

  /** Plan a single partition for aggregation pushdown. */
  private def planAggregationPartition(agg: Aggregation): Array[InputPartition] = {
    if (resolvedAggConfigs.isEmpty) {
      // Fallback to legacy count config for backwards compatibility
      planLegacyCountPartition()
    } else {
      logInfo(s"Planning aggregation partition for ${resolvedAggConfigs.size} aggregate(s)")

      // Single partition gets full rate limit
      val effectiveRateLimit = table.sourceConfig.http.rateLimit

      Array(AggregationInputPartition(
        aggregationConfigs = resolvedAggConfigs,
        sourceConfig = table.sourceConfig,
        baseUrl = table.baseUrl,
        pushedParams = pushedParams,
        effectiveRateLimit = effectiveRateLimit
      ))
    }
  }

  /** Plan a single partition for legacy COUNT(*) config. */
  private def planLegacyCountPartition(): Array[InputPartition] = {
    val tableConfig = table.tableConfig
      .getOrElse(throw new IllegalStateException("COUNT(*) pushed but no table config"))
    val countConfig = tableConfig.count
      .getOrElse(throw new IllegalStateException("COUNT(*) pushed but no count config"))

    logInfo("Planning COUNT(*) partition using legacy count endpoint")

    // Single partition gets full rate limit
    val effectiveRateLimit = table.sourceConfig.http.rateLimit

    Array(CountInputPartition(
      tableConfig = tableConfig,
      sourceConfig = table.sourceConfig,
      baseUrl = table.baseUrl,
      countConfig = countConfig,
      pushedParams = pushedParams,
      effectiveRateLimit = effectiveRateLimit
    ))
  }

  /** Plan partitions for regular data reads. */
  private def planDataPartitions(): Array[InputPartition] = {
    table.tableConfig.flatMap(_.partition) match {
      case Some(config: PartitionConfig.DateRange) => planDateRangePartitions(config)
      case Some(config: PartitionConfig.Enum)      => planEnumPartitions(config)
      case None =>
        // No partitioning configured - single partition gets full rate limit
        val effectiveRateLimit = table.sourceConfig.http.rateLimit
        Array(makePartition(pushedParams, effectiveRateLimit))
    }
  }

  /** Create multiple partitions for enum-based parallel reads. */
  private def planEnumPartitions(config: PartitionConfig.Enum): Array[InputPartition] = {
    val paramName = config.param
    val values = config.values
    val numPartitions = values.size
    val shares = rateLimitShares(numPartitions)

    shares match {
      case Some(s) =>
        logInfo(s"Partitioning by enum '$paramName' into $numPartitions partitions: ${values.mkString(", ")}, " +
          s"rate limit ${s.sum} rps distributed as ${s.mkString("+")}")
      case None =>
        logInfo(s"Partitioning by enum '$paramName' into $numPartitions partitions: ${values.mkString(", ")}")
    }

    values.zipWithIndex.map { case (value, idx) =>
      val partitionParams = pushedParams.updated(paramName, value)
      makePartition(partitionParams, shares.map(_(idx)))
    }.toArray
  }

  /** Create multiple partitions for date-range parallel reads. */
  private def planDateRangePartitions(config: PartitionConfig.DateRange): Array[InputPartition] = {
    // Extract date range from pushed params
    val startValue = pushedParams.get(config.startParam)
    val endValue = pushedParams.get(config.endParam)

    (startValue, endValue) match {
      case (Some(start), Some(end)) =>
        tryParseAndPartition(config, start, end).getOrElse(singlePartitionFallback())

      case _ =>
        // Date range not fully specified in query - fall back to single partition
        // This is a warning because user configured partitioning but it's not being used
        logWarning(s"Partition config specified but date range params " +
          s"'${config.startParam}' and '${config.endParam}' not found in query filters. " +
          "Using single partition. Ensure both predicates are pushed down via filter config.")
        singlePartitionFallback()
    }
  }

  /** Try to parse date range and create partitions. Returns None on parse errors. */
  private def tryParseAndPartition(
      config: PartitionConfig.DateRange,
      start: String,
      end: String
  ): Option[Array[InputPartition]] = {
    try {
      val formatter = DateTimeFormatter.ofPattern(config.format).withZone(ZoneOffset.UTC)
      val startInstant = Instant.from(formatter.parse(start))
      val endInstant = Instant.from(formatter.parse(end))
      val rangeMillis = config.range.toMillis

      // Generate partition ranges (iterative to avoid stack overflow)
      val ranges = generateRanges(startInstant.toEpochMilli, endInstant.toEpochMilli, rangeMillis)

      if (ranges.isEmpty) {
        logInfo("Date range is empty (start >= end), returning empty partition list")
        Some(Array.empty)
      } else {
        val numPartitions = ranges.size
        val shares = rateLimitShares(numPartitions)

        shares match {
          case Some(s) =>
            logInfo(s"Partitioning into $numPartitions date ranges (${config.range} each), " +
              s"rate limit ${s.sum} rps distributed as ${s.mkString("+")}")
          case None =>
            logInfo(s"Partitioning into $numPartitions date ranges (${config.range} each)")
        }

        Some(ranges.zipWithIndex.map { case ((rangeStart, rangeEnd), idx) =>
          val startStr = formatter.format(Instant.ofEpochMilli(rangeStart))
          val endStr = formatter.format(Instant.ofEpochMilli(rangeEnd))

          // Replace date range params with partition-specific values
          val partitionParams = pushedParams
            .updated(config.startParam, startStr)
            .updated(config.endParam, endStr)

          makePartition(partitionParams, shares.map(_(idx)))
        }.toArray)
      }
    } catch {
      case e: java.time.format.DateTimeParseException =>
        logWarning(s"Failed to parse date range values (start='$start', end='$end') " +
          s"with format '${config.format}': ${e.getMessage}. Using single partition.")
        None
    }
  }

  /** Single partition fallback when partitioning cannot be applied. */
  private def singlePartitionFallback(): Array[InputPartition] = {
    // Single partition gets full rate limit
    val effectiveRateLimit = table.sourceConfig.http.rateLimit
    Array(makePartition(pushedParams, effectiveRateLimit))
  }

  /** Create a RESTInputPartition with all common fields from this scan. */
  private def makePartition(
      params: Map[String, String],
      effectiveRateLimit: Option[Int]
  ): RESTInputPartition = {
    RESTInputPartition(
      endpoint = table.endpoint,
      tableConfig = table.tableConfig,
      sourceConfig = table.sourceConfig,
      baseUrl = table.baseUrl,
      arrowSchemaJson = arrowSchema.toJson,
      pushedParams = params,
      pushedLimit = pushedLimit,
      effectiveRateLimit = effectiveRateLimit,
      schemaMode = table.sourceConfig.schema.mode,
      responseFormat = table.sourceConfig.http.responseFormat,
      tableName = table.tableName,
      checkpointConfig = table.tableConfig.flatMap(_.checkpoint)
    )
  }

  /** Generate non-overlapping ranges covering [start, end).
    * Uses Iterator.unfold for stack safety with fine-grained partitions.
    */
  private def generateRanges(start: Long, end: Long, rangeSize: Long): List[(Long, Long)] = {
    Iterator.unfold(start) { s =>
      if (s >= end) None
      else {
        val rangeEnd = math.min(s + rangeSize, end)
        Some(((s, rangeEnd), rangeEnd))
      }
    }.toList
  }

  /** Calculate effective rate limit per partition.
    *
    * Divides the configured rate limit by the number of partitions to prevent
    * exceeding the API's global rate limit when partitions run in parallel.
    *
    * @param numPartitions Number of partitions that will run in parallel
    * @return Effective rate limit per partition, or None if no rate limit configured
    */
  /** Split the configured rate limit across partitions, one share each.
    *
    * Shares sum to exactly the configured limit. Plain integer division silently lost
    * the remainder — 15 rps over 4 partitions gave 3 each, using 12 of 15 — so the
    * remainder is handed out one per partition instead.
    *
    * This bounds the aggregate rate only while partitions run concurrently. If the
    * cluster runs fewer tasks at once the real rate is lower, never higher, so the
    * configured limit is still respected; it is a ceiling, not a target (#205).
    *
    * @throws IllegalArgumentException if the limit cannot cover one rps per partition
    */
  private def rateLimitShares(numPartitions: Int): Option[IndexedSeq[Int]] =
    table.sourceConfig.http.rateLimit.map { configuredLimit =>
      if (configuredLimit < numPartitions) {
        // Previously this fell back to 1 rps per partition and logged a warning, which
        // exceeded the very limit the setting exists to enforce. Fail at planning time
        // instead — a job that quietly doubles its request rate is worse than one that
        // refuses to start.
        throw new IllegalArgumentException(
          s"rate-limit is $configuredLimit rps but this scan plans $numPartitions partitions, " +
            s"so each would need less than 1 rps. Running anyway would allow up to $numPartitions rps, " +
            s"exceeding the configured limit. Raise rate-limit to at least $numPartitions, " +
            s"or reduce the partition count."
        )
      }
      val base      = configuredLimit / numPartitions
      val remainder = configuredLimit % numPartitions
      IndexedSeq.tabulate(numPartitions)(i => base + (if (i < remainder) 1 else 0))
    }

  override def createReaderFactory(): PartitionReaderFactory =
    new RESTPartitionReaderFactory()

  /** Report statistics to Spark for query optimization. */
  override def estimateStatistics(): Statistics =
    ScanStatistics.estimate(pushedLimit, table.sourceConfig.pagination, readSchema())
}

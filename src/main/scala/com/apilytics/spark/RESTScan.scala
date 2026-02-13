package com.apilytics.spark

import com.apilytics.core.config.PartitionConfig
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

class RESTScan(
    table: RESTTable,
    arrowSchema: ArrowSchema,
    prunedSchema: Option[StructType],
    pushedParams: Map[String, String],
    pushedLimit: Option[Int],
    pushedAggregation: Option[Aggregation] = None
) extends Scan with Batch with Logging {

  // If pruned, return the pruned schema; otherwise return full schema
  override def readSchema(): StructType = prunedSchema.getOrElse(table.schema())

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // If aggregation is pushed, return a single count partition
    pushedAggregation match {
      case Some(_) =>
        planCountPartition()
      case None =>
        planDataPartitions()
    }
  }

  /** Plan a single partition for COUNT(*) aggregation. */
  private def planCountPartition(): Array[InputPartition] = {
    val tableConfig = table.tableConfig
      .getOrElse(throw new IllegalStateException("COUNT(*) pushed but no table config"))
    val countConfig = tableConfig.count
      .getOrElse(throw new IllegalStateException("COUNT(*) pushed but no count config"))

    logInfo("Planning COUNT(*) partition using count endpoint")

    Array(CountInputPartition(
      tableConfig = tableConfig,
      sourceConfig = table.sourceConfig,
      baseUrl = table.baseUrl,
      countConfig = countConfig,
      pushedParams = pushedParams
    ))
  }

  /** Plan partitions for regular data reads. */
  private def planDataPartitions(): Array[InputPartition] = {
    table.tableConfig.flatMap(_.partition) match {
      case Some(partitionConfig) =>
        planDateRangePartitions(partitionConfig)
      case None =>
        // No partitioning configured - single partition
        Array(RESTInputPartition(
          endpoint = table.endpoint,
          tableConfig = table.tableConfig,
          sourceConfig = table.sourceConfig,
          baseUrl = table.baseUrl,
          arrowSchemaJson = arrowSchema.toJson,
          pushedParams = pushedParams,
          pushedLimit = pushedLimit
        ))
    }
  }

  /** Create multiple partitions for date-range parallel reads. */
  private def planDateRangePartitions(config: PartitionConfig): Array[InputPartition] = {
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
      config: PartitionConfig,
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
        logInfo(s"Partitioning into ${ranges.size} date ranges (${config.range} each)")

        Some(ranges.map { case (rangeStart, rangeEnd) =>
          val startStr = formatter.format(Instant.ofEpochMilli(rangeStart))
          val endStr = formatter.format(Instant.ofEpochMilli(rangeEnd))

          // Replace date range params with partition-specific values
          val partitionParams = pushedParams
            .updated(config.startParam, startStr)
            .updated(config.endParam, endStr)

          RESTInputPartition(
            endpoint = table.endpoint,
            tableConfig = table.tableConfig,
            sourceConfig = table.sourceConfig,
            baseUrl = table.baseUrl,
            arrowSchemaJson = arrowSchema.toJson,
            pushedParams = partitionParams,
            pushedLimit = pushedLimit
          )
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
    Array(RESTInputPartition(
      endpoint = table.endpoint,
      tableConfig = table.tableConfig,
      sourceConfig = table.sourceConfig,
      baseUrl = table.baseUrl,
      arrowSchemaJson = arrowSchema.toJson,
      pushedParams = pushedParams,
      pushedLimit = pushedLimit
    ))
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

  override def createReaderFactory(): PartitionReaderFactory =
    new RESTPartitionReaderFactory()
}

package com.apilytics.spark

import com.apilytics.core.config.PartitionConfig
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

class RESTScan(
    table: RESTTable,
    arrowSchema: ArrowSchema,
    prunedSchema: Option[StructType],
    pushedParams: Map[String, String],
    pushedLimit: Option[Int]
) extends Scan with Batch with Logging {

  // If pruned, return the pruned schema; otherwise return full schema
  override def readSchema(): StructType = prunedSchema.getOrElse(table.schema())

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
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
        val formatter = DateTimeFormatter.ofPattern(config.format).withZone(ZoneOffset.UTC)
        val startInstant = Instant.from(formatter.parse(start))
        val endInstant = Instant.from(formatter.parse(end))
        val rangeMillis = config.range.toMillis

        // Generate partition ranges
        val ranges = generateRanges(startInstant.toEpochMilli, endInstant.toEpochMilli, rangeMillis)
        logInfo(s"Partitioning into ${ranges.size} date ranges (${config.range} each)")

        ranges.map { case (rangeStart, rangeEnd) =>
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
        }.toArray

      case _ =>
        // Date range not fully specified in query - fall back to single partition
        logInfo("Date range not specified in query, using single partition")
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

  /** Generate non-overlapping ranges covering [start, end). */
  private def generateRanges(start: Long, end: Long, rangeSize: Long): List[(Long, Long)] = {
    if (start >= end) Nil
    else {
      val rangeEnd = math.min(start + rangeSize, end)
      (start, rangeEnd) :: generateRanges(rangeEnd, end, rangeSize)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new RESTPartitionReaderFactory()
}

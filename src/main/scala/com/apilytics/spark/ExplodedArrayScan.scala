package com.apilytics.spark

import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.types.StructType

class ExplodedArrayScan(
    table: ExplodedArrayTable,
    arrowSchema: ArrowSchema,
    prunedSchema: Option[StructType],
    pushedParams: Map[String, String],
    pushedLimit: Option[Int]
) extends Scan with Batch with SupportsReportStatistics {

  /** Counts of converted and NULLed values, in the SQL tab next to the scan (#309). */
  override def supportedCustomMetrics(): Array[CustomMetric] = ConversionMetrics.supported

  override def readSchema(): StructType = prunedSchema.getOrElse(table.schema())

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] =
    Array(ExplodedArrayInputPartition(
      handle = table.handle,
      tableConfig = table.tableConfig,
      sourceConfig = table.sourceConfig,
      baseUrl = table.baseUrl,
      arrowSchemaJson = arrowSchema.toJson,
      arrayFieldName = table.arrayFieldName,
      arrayJsonPath = table.arrayFieldInfo.jsonPath,
      pushedParams = pushedParams,
      pushedLimit = pushedLimit
    ))

  override def createReaderFactory(): PartitionReaderFactory =
    new ExplodedArrayPartitionReaderFactory()

  /** Report statistics to Spark for query optimization. */
  override def estimateStatistics(): Statistics =
    ScanStatistics.estimate(pushedLimit, table.sourceConfig.pagination, readSchema())
}

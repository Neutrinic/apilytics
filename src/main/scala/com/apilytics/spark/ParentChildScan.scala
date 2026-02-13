package com.apilytics.spark

import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

class ParentChildScan(
    table: ParentChildTable,
    arrowSchema: ArrowSchema,
    prunedSchema: Option[StructType],
    pushedParams: Map[String, String],
    pushedLimit: Option[Int]
) extends Scan with Batch {

  override def readSchema(): StructType = prunedSchema.getOrElse(table.schema())

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // Single partition for parent-child tables gets the full rate limit
    val effectiveRateLimit = table.sourceConfig.http.rateLimit

    Array(ParentChildInputPartition(
      childEndpointTemplate = table.childEndpointTemplate,
      parentEndpoint = table.parentEndpoint,
      childResponseSchema = table.childResponseSchema,
      parentKey = table.parentKey,
      pathParamName = table.pathParamName,
      parentKeyColumn = table.parentKeyColumn,
      tableConfig = table.tableConfig,
      sourceConfig = table.sourceConfig,
      baseUrl = table.baseUrl,
      arrowSchemaJson = arrowSchema.toJson,
      pushedParams = pushedParams,
      pushedLimit = pushedLimit,
      effectiveRateLimit = effectiveRateLimit
    ))
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new ParentChildPartitionReaderFactory()
}

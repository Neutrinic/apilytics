package com.apilytics.spark

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

class RESTScan(
    table: RESTTable,
    pushedParams: Map[String, String],
    pushedLimit: Option[Int]
) extends Scan with Batch {

  override def readSchema(): StructType = table.schema()

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] =
    Array(RESTInputPartition(
      endpoint = table.endpoint,
      tableConfig = table.tableConfig,
      sourceConfig = table.sourceConfig,
      baseUrl = table.baseUrl,
      arrowSchemaJson = table.arrowSchema.toJson,
      pushedParams = pushedParams,
      pushedLimit = pushedLimit
    ))

  override def createReaderFactory(): PartitionReaderFactory =
    new RESTPartitionReaderFactory()
}

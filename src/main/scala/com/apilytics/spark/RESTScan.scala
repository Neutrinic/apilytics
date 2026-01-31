package com.apilytics.spark

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

class RESTScan(table: RESTTable) extends Scan with Batch {

  override def readSchema(): StructType = table.schema()

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] =
    Array(RESTInputPartition(
      endpoint = table.endpoint,
      tableConfig = table.tableConfig,
      sourceConfig = table.sourceConfig,
      baseUrl = table.baseUrl,
      arrowSchema = table.arrowSchema,
      pushedParams = Map.empty,
      pushedLimit = None
    ))

  override def createReaderFactory(): PartitionReaderFactory =
    new RESTPartitionReaderFactory()
}

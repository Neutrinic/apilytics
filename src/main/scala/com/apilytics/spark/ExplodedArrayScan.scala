package com.apilytics.spark

import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

class ExplodedArrayScan(
    table: ExplodedArrayTable,
    arrowSchema: ArrowSchema
) extends Scan with Batch {

  override def readSchema(): StructType = table.schema()

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] =
    Array(ExplodedArrayInputPartition(
      endpoint = table.endpoint,
      tableConfig = table.tableConfig,
      sourceConfig = table.sourceConfig,
      baseUrl = table.baseUrl,
      arrowSchemaJson = arrowSchema.toJson,
      arrayFieldName = table.arrayFieldName,
      arrayJsonPath = table.arrayFieldInfo.jsonPath
    ))

  override def createReaderFactory(): PartitionReaderFactory =
    new ExplodedArrayPartitionReaderFactory()
}

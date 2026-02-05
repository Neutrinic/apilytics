package com.apilytics.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

class ExplodedArrayPartitionReaderFactory extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val p = partition.asInstanceOf[ExplodedArrayInputPartition]
    new ExplodedArrayColumnarPartitionReader(p)
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[ExplodedArrayInputPartition]
    new ExplodedArrayPartitionReader(p)
  }
}

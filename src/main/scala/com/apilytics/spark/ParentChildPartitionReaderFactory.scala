package com.apilytics.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

class ParentChildPartitionReaderFactory extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case p: ParentChildInputPartition => new ParentChildPartitionReader(p)
      case _ => throw new IllegalArgumentException(s"Unexpected partition type: ${partition.getClass}")
    }
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    partition match {
      case p: ParentChildInputPartition => new ParentChildColumnarPartitionReader(p)
      case _ => throw new IllegalArgumentException(s"Unexpected partition type: ${partition.getClass}")
    }
  }
}

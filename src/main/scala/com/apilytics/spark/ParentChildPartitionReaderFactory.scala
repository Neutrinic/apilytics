package com.apilytics.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

class ParentChildPartitionReaderFactory extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  // Abstract in PartitionReaderFactory so it has to exist, but Spark never calls it
  // while supportColumnarReads is true. The row-based reader that used to live here was
  // unreachable from birth — this factory shipped with the flag already true (#211).
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException(
      "Parent-child joins are read as columnar batches only"
    )

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    partition match {
      case p: ParentChildInputPartition => new ParentChildColumnarPartitionReader(p)
      case _ => throw new IllegalArgumentException(s"Unexpected partition type: ${partition.getClass}")
    }
  }
}

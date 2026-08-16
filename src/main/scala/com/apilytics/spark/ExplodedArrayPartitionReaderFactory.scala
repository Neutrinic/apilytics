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

  // Abstract in PartitionReaderFactory so it has to exist, but Spark never calls it
  // while supportColumnarReads is true. A throw states that; the row-based reader that
  // used to live here was unreachable from the commit that added columnar reads (#211).
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException(
      "Exploded array views are read as columnar batches only"
    )
}

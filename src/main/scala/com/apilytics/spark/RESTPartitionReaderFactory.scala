package com.apilytics.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

class RESTPartitionReaderFactory extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    // Aggregation partitions return a single row, use row-based reader
    partition match {
      case _: CountInputPartition       => false
      case _: AggregationInputPartition => false
      case _                            => true
    }
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    partition match {
      case p: RESTInputPartition => new RESTColumnarPartitionReader(p)
      case other => throw new IllegalArgumentException(
        s"Columnar reader not supported for ${other.getClass.getSimpleName}"
      )
    }
  }

  // Only aggregate partitions reach this: supportColumnarReads returns false for them
  // and true for everything else, so a RESTInputPartition is always read columnar (#211).
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case p: CountInputPartition       => new CountPartitionReader(p)
      case p: AggregationInputPartition => new AggregationPartitionReader(p)
      case other => throw new IllegalArgumentException(
        s"Unknown partition type: ${other.getClass.getSimpleName}"
      )
    }
  }
}

package com.apilytics.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

class RESTPartitionReaderFactory extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    // CountInputPartition returns a single row, use row-based reader
    partition match {
      case _: CountInputPartition => false
      case _                      => true
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

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case p: RESTInputPartition   => new RESTPartitionReader(p)
      case p: CountInputPartition  => new CountPartitionReader(p)
      case other => throw new IllegalArgumentException(
        s"Unknown partition type: ${other.getClass.getSimpleName}"
      )
    }
  }
}

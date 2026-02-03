package com.apilytics.spark

import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}

class ExplodedArrayScanBuilder(
    table: ExplodedArrayTable,
    arrowSchema: ArrowSchema
) extends ScanBuilder {

  override def build(): Scan = new ExplodedArrayScan(table, arrowSchema)
}

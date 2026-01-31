package com.apilytics.spark

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}

class RESTScanBuilder(table: RESTTable) extends ScanBuilder {

  override def build(): Scan = new RESTScan(table)
}

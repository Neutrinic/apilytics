package com.apilytics.spark

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}

class RESTScanBuilder(table: RESTTable) extends ScanBuilder {

  private var pushedParams: Map[String, String] = Map.empty
  private var pushedLimit: Option[Int] = None

  override def build(): Scan = new RESTScan(table, pushedParams, pushedLimit)
}

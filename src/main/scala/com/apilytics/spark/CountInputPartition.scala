package com.apilytics.spark

import com.apilytics.core.config.{CountConfig, SourceConfig, TableConfig}
import org.apache.spark.sql.connector.read.InputPartition

/** Input partition for COUNT(*) aggregation pushdown.
  *
  * Instead of fetching all data and counting locally, this partition
  * calls a count endpoint or uses a count query parameter to get
  * the count directly from the API.
  */
case class CountInputPartition(
    tableConfig: TableConfig,
    sourceConfig: SourceConfig,
    baseUrl: String,
    countConfig: CountConfig,
    pushedParams: Map[String, String]
) extends InputPartition

package com.apilytics.spark

import com.apilytics.core.config.{SourceConfig, TableConfig}
import com.apilytics.core.openapi.Endpoint
import org.apache.spark.sql.connector.read.InputPartition

case class RESTInputPartition(
    endpoint: Endpoint,
    tableConfig: Option[TableConfig],
    sourceConfig: SourceConfig,
    baseUrl: String,
    arrowSchemaJson: String,
    pushedParams: Map[String, String],
    pushedLimit: Option[Int]
) extends InputPartition

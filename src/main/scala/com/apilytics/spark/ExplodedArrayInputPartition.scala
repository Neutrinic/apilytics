package com.apilytics.spark

import com.apilytics.core.config.{SourceConfig, TableConfig}
import com.apilytics.core.openapi.Endpoint
import org.apache.spark.sql.connector.read.InputPartition

case class ExplodedArrayInputPartition(
    endpoint: Endpoint,
    tableConfig: Option[TableConfig],
    sourceConfig: SourceConfig,
    baseUrl: String,
    arrowSchemaJson: String,
    arrayFieldName: String,
    arrayJsonPath: List[String]
) extends InputPartition

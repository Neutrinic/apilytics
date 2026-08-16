package com.apilytics.spark

import com.apilytics.core.config.{SourceConfig, TableConfig}
import com.apilytics.core.source.SourceHandle
import org.apache.spark.sql.connector.read.InputPartition

case class ExplodedArrayInputPartition(
    handle: SourceHandle,
    tableConfig: Option[TableConfig],
    sourceConfig: SourceConfig,
    baseUrl: String,
    arrowSchemaJson: String,
    arrayFieldName: String,
    arrayJsonPath: List[String],
    pushedParams: Map[String, String] = Map.empty,
    pushedLimit: Option[Int] = None
) extends InputPartition

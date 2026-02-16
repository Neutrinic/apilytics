package com.apilytics.spark

import com.apilytics.core.config.{SchemaMode, SourceConfig, TableConfig}
import com.apilytics.core.openapi.Endpoint
import org.apache.spark.sql.connector.read.InputPartition

case class RESTInputPartition(
    endpoint: Endpoint,
    tableConfig: Option[TableConfig],
    sourceConfig: SourceConfig,
    baseUrl: String,
    arrowSchemaJson: String,
    pushedParams: Map[String, String],
    pushedLimit: Option[Int],
    /** Effective rate limit for this partition (configured limit / number of partitions).
      * Automatically distributed by RESTScan to prevent exceeding API limits. */
    effectiveRateLimit: Option[Int] = None,
    /** Schema mode - determines whether to use Arrow path or native Variant. */
    schemaMode: SchemaMode = SchemaMode.Strict
) extends InputPartition

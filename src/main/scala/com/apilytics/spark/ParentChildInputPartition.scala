package com.apilytics.spark

import com.apilytics.core.config.{SourceConfig, TableConfig}
import com.apilytics.core.openapi.{Endpoint, OpenAPISchema}
import org.apache.spark.sql.connector.read.InputPartition

case class ParentChildInputPartition(
    childEndpointTemplate: String,
    parentEndpoint: Endpoint,
    childResponseSchema: OpenAPISchema.ObjectType,
    parentKey: String,
    pathParamName: String,
    parentKeyColumn: String,
    tableConfig: TableConfig,
    sourceConfig: SourceConfig,
    baseUrl: String,
    arrowSchemaJson: String,
    pushedParams: Map[String, String] = Map.empty,
    pushedLimit: Option[Int] = None,
    /** Effective rate limit for this partition (configured limit / number of partitions).
      * Automatically distributed by ParentChildScan to prevent exceeding API limits. */
    effectiveRateLimit: Option[Int] = None
) extends InputPartition

package com.apilytics.spark

import com.apilytics.core.config.{CheckpointConfig, ResponseFormat, SchemaMode, SourceConfig, TableConfig}
import com.apilytics.core.source.SourceHandle
import org.apache.spark.sql.connector.read.InputPartition

case class RESTInputPartition(
    handle: SourceHandle,
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
    schemaMode: SchemaMode = SchemaMode.Strict,
    /** Response format for HTTP responses (json, ndjson, sse). */
    responseFormat: ResponseFormat = ResponseFormat.Json,
    /** Table name for checkpoint file naming. */
    tableName: String = "",
    /** Checkpoint configuration for incremental reads. */
    checkpointConfig: Option[CheckpointConfig] = None,
    /** Exclusive upper bound for a streaming micro-batch.
      *
      * The API filter is one-sided, so a request for "changed since X" also returns
      * records newer than this batch's end offset. Without trimming them the next batch
      * returns them again. Set only for streaming reads. */
    streamBound: Option[StreamBound] = None
) extends InputPartition

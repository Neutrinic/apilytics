package com.apilytics.spark

import com.apilytics.core.config.{AggregationConfig, SourceConfig}
import org.apache.spark.sql.connector.read.InputPartition

/** Input partition for aggregation pushdown.
  *
  * Instead of fetching all data and aggregating locally, this partition
  * calls configured aggregation endpoints to get results directly from the API.
  *
  * Supports multiple aggregations in a single query (e.g., SELECT SUM(a), AVG(b)).
  */
case class AggregationInputPartition(
    /** Resolved aggregation configs for each pushed aggregate. */
    aggregationConfigs: List[AggregationConfig],
    sourceConfig: SourceConfig,
    baseUrl: String,
    pushedParams: Map[String, String],
    /** Effective rate limit for this partition. */
    effectiveRateLimit: Option[Int] = None
) extends InputPartition

package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.apilytics.core.config.AggregationConfig
import com.apilytics.core.http.Client
import com.apilytics.core.http.Client.RestClient
import io.circe.Json
import io.circe.pointer.Pointer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String
import org.http4s.Uri

/** Partition reader for aggregation pushdown.
  *
  * Makes HTTP requests to aggregation endpoints and returns a single row
  * with all aggregation results.
  *
  * For multiple aggregations, each is fetched from its configured endpoint.
  * Results are returned as a single row with values in config order.
  */
class AggregationPartitionReader(partition: AggregationInputPartition) extends PartitionReader[InternalRow] {

  private val results: Array[Any] = fetchAggregations()
  private var returned = false

  override def next(): Boolean = {
    if (!returned) {
      returned = true
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = InternalRow.fromSeq(results.toSeq)

  override def close(): Unit = ()

  /** Fetch all aggregation results.
    *
    * Optimizes by grouping configs with the same endpoint+params, making one
    * HTTP call per unique combination, then extracting multiple values.
    */
  private def fetchAggregations(): Array[Any] = {
    // Use effective rate limit
    val httpConfig = partition.effectiveRateLimit match {
      case Some(_) => partition.sourceConfig.http.copy(rateLimit = partition.effectiveRateLimit)
      case None    => partition.sourceConfig.http
    }

    // Index configs to preserve original order in results
    val indexedConfigs = partition.aggregationConfigs.zipWithIndex

    // Group by (endpoint, merged params) to deduplicate HTTP calls
    val grouped = indexedConfigs.groupBy { case (config, _) =>
      (config.endpoint, partition.pushedParams ++ config.params)
    }

    val program: IO[Array[Any]] = Client
      .resource(httpConfig, partition.sourceConfig.auth)
      .use { client =>
        // Fetch once per unique endpoint+params, extract all response paths
        grouped.toList.traverse { case ((endpoint, params), configsWithIndex) =>
          fetchAndExtract(client, endpoint, params, configsWithIndex)
        }.map { results =>
          // Flatten and sort by original index to restore order
          results.flatten.sortBy(_._1).map(_._2).toArray
        }
      }

    program.unsafeRunSync()
  }

  /** Fetch endpoint once and extract multiple values for all configs sharing it. */
  private def fetchAndExtract(
      client: RestClient,
      endpoint: String,
      params: Map[String, String],
      configsWithIndex: List[(AggregationConfig, Int)]
  ): IO[List[(Int, Any)]] = {
    val uri = Uri.unsafeFromString(partition.baseUrl + endpoint)

    client.get(uri, params).map { response =>
      configsWithIndex.map { case (config, idx) =>
        idx -> extractValue(response.json, config.responsePath)
      }
    }
  }

  /** Extract value from JSON using JSON pointer. */
  private def extractValue(json: Json, responsePath: String): Any = {
    val pointer = Pointer.parse(responsePath).getOrElse(
      throw new IllegalArgumentException(s"Invalid JSON pointer: $responsePath")
    )

    pointer.get(json) match {
      case Right(valueJson) =>
        jsonToScala(valueJson)
      case Left(_) =>
        throw new RuntimeException(
          s"Value not found at '$responsePath' in response: $json"
        )
    }
  }

  /** Convert JSON value to Spark InternalRow types.
    * Strings must be UTF8String for Spark compatibility.
    */
  private def jsonToScala(json: Json): Any = {
    json.fold(
      jsonNull = null,
      jsonBoolean = identity,
      jsonNumber = n => n.toLong.getOrElse(n.toDouble),
      jsonString = s => UTF8String.fromString(s),
      jsonArray = arr => arr.map(jsonToScala).toArray,
      jsonObject = obj => obj.toMap.map { case (k, v) => k -> jsonToScala(v) }
    )
  }
}

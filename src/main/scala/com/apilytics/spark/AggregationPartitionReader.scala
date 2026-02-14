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

  /** Fetch all aggregation results. */
  private def fetchAggregations(): Array[Any] = {
    // Use effective rate limit
    val httpConfig = partition.effectiveRateLimit match {
      case Some(_) => partition.sourceConfig.http.copy(rateLimit = partition.effectiveRateLimit)
      case None    => partition.sourceConfig.http
    }

    val program: IO[Array[Any]] = Client
      .resource(httpConfig, partition.sourceConfig.auth)
      .use { client =>
        // Fetch each aggregation
        // TODO: Optimize by grouping configs with same endpoint
        partition.aggregationConfigs.traverse { config =>
          fetchSingleAggregation(client, config)
        }.map(_.toArray)
      }

    program.unsafeRunSync()
  }

  /** Fetch a single aggregation result. */
  private def fetchSingleAggregation(client: RestClient, config: AggregationConfig): IO[Any] = {
    val uri = Uri.unsafeFromString(partition.baseUrl + config.endpoint)

    // Merge pushed params with config params
    val params = partition.pushedParams ++ config.params

    client.get(uri, params).map { response =>
      extractValue(response.json, config.responsePath)
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

package com.apilytics.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.http.Client
import io.circe.Json
import io.circe.pointer.Pointer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.http4s.Uri

/** Partition reader for COUNT(*) aggregation pushdown.
  *
  * Makes a single HTTP request to the count endpoint and returns
  * a single row with the count value.
  *
  * Note: Pushed filter params are forwarded as-is to the count endpoint.
  * If the count endpoint doesn't support the same filter params as the
  * main endpoint, you may get unexpected results or errors.
  */
class CountPartitionReader(partition: CountInputPartition) extends PartitionReader[InternalRow] {

  private val count: Long = fetchCount()
  private var returned = false

  override def next(): Boolean = {
    if (!returned) {
      returned = true
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = InternalRow(count)

  override def close(): Unit = ()

  private def fetchCount(): Long = {
    val config = partition.countConfig

    // Determine endpoint: use count endpoint if specified, otherwise main endpoint with param
    val endpoint = config.endpoint.getOrElse(partition.tableConfig.endpoint)

    // Build URI
    val uri = Uri.unsafeFromString(partition.baseUrl + endpoint)

    // Add count param if configured (when not using dedicated count endpoint)
    val params: Map[String, String] = config.param match {
      case Some(paramName) =>
        partition.pushedParams + (paramName -> config.paramValue)
      case None =>
        partition.pushedParams
    }

    // Use effective rate limit (typically full limit for single COUNT partition)
    val httpConfig = partition.effectiveRateLimit match {
      case Some(_) => partition.sourceConfig.http.copy(rateLimit = partition.effectiveRateLimit)
      case None    => partition.sourceConfig.http
    }

    // Make request
    val program: IO[Long] = Client
      .resource(httpConfig, partition.sourceConfig.auth)
      .use { client =>
        client.get(uri, params).map { response =>
          extractCount(response.json, config.responsePath)
        }
      }

    program.unsafeRunSync()
  }

  /** Extract count value from JSON using JSON pointer. */
  private def extractCount(json: Json, responsePath: String): Long = {
    val pointer = Pointer.parse(responsePath).getOrElse(
      throw new IllegalArgumentException(s"Invalid JSON pointer: $responsePath")
    )

    pointer.get(json) match {
      case Right(countJson) =>
        countJson.asNumber
          .flatMap(_.toLong)
          .getOrElse(throw new RuntimeException(
            s"Count value at '$responsePath' is not a number: $countJson"
          ))
      case Left(_) =>
        throw new RuntimeException(
          s"Count field not found at '$responsePath' in response: $json"
        )
    }
  }
}

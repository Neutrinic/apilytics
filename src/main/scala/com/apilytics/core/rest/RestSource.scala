package com.apilytics.core.rest

import cats.effect.{IO, Resource}
import com.apilytics.core.arrow.Converter
import com.apilytics.core.config.{ResponseFormat, SourceConfig, TableConfig}
import com.apilytics.core.http.{Client, Paginator, ResponseCache}
import com.apilytics.core.openapi.Endpoint
import com.apilytics.core.source.{ReadRequest, RecordPage, RecordSession, RecordSource, SourceHandle}
import fs2.Stream
import org.http4s.Uri

/** REST's identity for one table: which endpoint, on which base URL, with what
  * table-level configuration.
  *
  * Opaque to the Spark layer — it only ever passes this back in a [[ReadRequest]].
  */
final case class RestHandle(
    endpoint: Endpoint,
    baseUrl: String,
    tableConfig: Option[TableConfig]
) extends SourceHandle

/** REST implementation of the protocol-neutral read interface (#191).
  *
  * Owns everything HTTP: client lifecycle, pagination, response format, and pulling
  * records out of a response body. None of that is visible to callers.
  *
  * @param sourceConfig the config for this read. Per-partition adjustments — notably
  *                     the effective rate limit — should already be applied here.
  */
final class RestSource(sourceConfig: SourceConfig) extends RecordSource {

  override def session: Resource[IO, RecordSession] = {
    // Built inside the resource so it is created on the executor, not serialised.
    val cache = ResponseCache.fromConfig(sourceConfig.http.responseCache)
    Client
      .resource(sourceConfig.http, sourceConfig.auth, cache)
      .map(client => new RestSession(client, sourceConfig))
  }
}

private[rest] final class RestSession(
    client: Client.RestClient,
    sourceConfig: SourceConfig
) extends RecordSession {

  override def pages(request: ReadRequest): Stream[IO, RecordPage] = {
    val handle = request.handle match {
      case h: RestHandle => h
      case other =>
        // A handle from another protocol reaching a REST session is a wiring bug,
        // not a user error — fail loudly rather than silently reading nothing.
        return Stream.raiseError[IO](
          new IllegalArgumentException(
            s"RestSource cannot read a ${other.getClass.getSimpleName}; " +
              "handles must come from the source that produced them"
          )
        )
    }

    val uri    = Uri.unsafeFromString(handle.baseUrl + handle.endpoint.path)
    val format = sourceConfig.http.responseFormat

    // For streaming formats (NDJSON, SSE) every element is already one record, so
    // data-path extraction does not apply — it only addresses records nested inside a
    // wrapper object, e.g. {"results": [...], "next": "..."}.
    val dataPath =
      if (format != ResponseFormat.Json) None else handle.tableConfig.flatMap(_.dataPath)

    Paginator
      .pagesWithState(
        client,
        uri,
        request.params,
        sourceConfig.pagination,
        request.limit,
        format,
        request.startState
      )
      .map { case (pageJson, state) =>
        RecordPage(Converter.extractRecords(pageJson, dataPath), state)
      }
  }
}

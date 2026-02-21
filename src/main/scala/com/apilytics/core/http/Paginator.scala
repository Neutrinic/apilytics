package com.apilytics.core.http

import cats.effect.IO
import com.apilytics.core.checkpoint.CheckpointState
import com.apilytics.core.config.{PaginationConfig, PaginationStyle, ResponseFormat}
import fs2.Stream
import io.circe.Json
import io.circe.pointer.Pointer
import org.http4s.Uri

object Paginator {

  /** Stream of JSON pages from a paginated API endpoint.
    *
    * For standard JSON format, pages are fetched according to the pagination style.
    * For streaming formats (NDJSON, SSE), records stream directly without pagination
    * since these represent continuous data streams.
    */
  def pages(
      client: Client.RestClient,
      baseUri: Uri,
      params: Map[String, String],
      pagination: PaginationConfig,
      limit: Option[Int] = None,
      format: ResponseFormat = ResponseFormat.Json
  ): Stream[IO, Json] =
    pagesWithState(client, baseUri, params, pagination, limit, format).map(_._1)

  /** Stream of (page JSON, checkpoint state) tuples for checkpoint-aware readers.
    *
    * Checkpoint state is emitted per page for cursor/offset pagination.
    * Link-header, single-page, and streaming formats emit None state.
    *
    * @param startState Optional checkpoint state to resume from (incremental reads).
    */
  def pagesWithState(
      client: Client.RestClient,
      baseUri: Uri,
      params: Map[String, String],
      pagination: PaginationConfig,
      limit: Option[Int] = None,
      format: ResponseFormat = ResponseFormat.Json,
      startState: Option[CheckpointState] = None
  ): Stream[IO, (Json, Option[CheckpointState])] = {
    format match {
      case ResponseFormat.Json =>
        // Standard full-body JSON with pagination
        pagination.style match {
          case PaginationStyle.Cursor =>
            cursorPages(client, baseUri, params, pagination, limit, startState)
          case PaginationStyle.Offset =>
            offsetPages(client, baseUri, params, pagination, limit, startState)
          case PaginationStyle.LinkHeader =>
            linkHeaderPages(client, baseUri, params, pagination, limit)
          case PaginationStyle.None =>
            singlePage(client, baseUri, params)
        }

      case streaming =>
        // Streaming formats bypass pagination - records stream directly
        // Apply limit if specified (take N records from stream)
        val stream = client.getStreaming(baseUri, params, streaming)
        val limited = limit.fold(stream)(n => stream.take(n.toLong))
        limited.map(json => (json, None))
    }
  }

  private def singlePage(
      client: Client.RestClient,
      baseUri: Uri,
      params: Map[String, String]
  ): Stream[IO, (Json, Option[CheckpointState])] = {
    Stream.eval(client.get(baseUri, params).map(resp => (resp.json, None)))
  }

  private def cursorPages(
      client: Client.RestClient,
      baseUri: Uri,
      params: Map[String, String],
      config: PaginationConfig,
      limit: Option[Int],
      startState: Option[CheckpointState]
  ): Stream[IO, (Json, Option[CheckpointState])] = {
    val cursorPath = config.cursorPath
      .flatMap(p => Pointer.parse(p).toOption)
      .getOrElse(throw new IllegalArgumentException("Cursor pagination requires cursor-path"))
    val cursorParam = config.cursorParam.getOrElse("cursor")
    val pageSizeParam = config.pageSizeParam

    val pageParams = pageSizeParam.map { psp =>
      val size = limit.map(l => math.min(l, config.maxPageSize)).getOrElse(config.maxPageSize)
      params + (psp -> size.toString)
    }.getOrElse(params)

    // Resume from checkpoint cursor if available
    val initialCursor: Option[String] = startState match {
      case Some(CheckpointState.CursorValue(c)) => Some(c)
      case _ => None
    }

    // State: Some(cursor) = next page with cursor, None = first page
    // unfoldEval stops when we return None
    def fetch(maybeCursor: Option[String]): IO[Option[(Json, Option[String])]] = {
      val reqParams = maybeCursor.fold(pageParams)(c => pageParams + (cursorParam -> c))
      client.get(baseUri, reqParams).map { resp =>
        val nextCursor = cursorPath.get(resp.json).toOption.flatMap(_.asString).filter(_.nonEmpty)
        Some((resp.json, nextCursor))
      }
    }

    // Use Stream.unfoldLoopEval: emit page, continue if nextCursor is Some
    Stream.eval(fetch(initialCursor)).flatMap {
      case None => Stream.empty
      case Some((firstPage, nextCursor)) =>
        val firstState = nextCursor.map(CheckpointState.CursorValue)
          .orElse(initialCursor.map(CheckpointState.CursorValue))
        Stream.emit((firstPage, firstState)) ++ Stream.unfoldEval(nextCursor) {
          case None => IO.pure(None)
          case Some(cursor) =>
            fetch(Some(cursor)).map {
              case Some((page, next)) =>
                val state: Option[CheckpointState] = next.map(CheckpointState.CursorValue)
                  .orElse(Some(CheckpointState.CursorValue(cursor)))
                Some(((page, state), next))
              case None => None
            }
        }
    }.through(limitPages(limit, config))
  }

  private def offsetPages(
      client: Client.RestClient,
      baseUri: Uri,
      params: Map[String, String],
      config: PaginationConfig,
      limit: Option[Int],
      startState: Option[CheckpointState]
  ): Stream[IO, (Json, Option[CheckpointState])] = {
    val offsetParam = config.offsetParam.getOrElse("offset")
    val pageSizeParam = config.pageSizeParam.getOrElse("limit")
    val pageSize = limit.map(l => math.min(l, config.maxPageSize)).getOrElse(config.maxPageSize)
    val resultsPointer = config.resultsPath.flatMap(p => Pointer.parse(p).toOption)

    // Resume from checkpoint offset if available
    val initialOffset: Int = startState match {
      case Some(CheckpointState.OffsetValue(v)) => v.toInt
      case _ => 0
    }

    Stream.unfoldEval[IO, Int, (Json, Option[CheckpointState])](initialOffset) { offset =>
      val reqParams = params + (offsetParam -> offset.toString) + (pageSizeParam -> pageSize.toString)
      client.get(baseUri, reqParams).map { resp =>
        val json = resp.json
        val isEmpty = resultsPointer match {
          case Some(ptr) =>
            // Check the configured results-path for an empty array
            ptr.get(json).toOption match {
              case Some(arr) => arr.asArray.exists(_.isEmpty)
              case None      => true // Path not found — no more data
            }
          case None =>
            // No results-path — check if top-level response is an empty array
            json.asArray.exists(_.isEmpty)
        }
        if (isEmpty) None
        else {
          val nextOffset = offset + pageSize
          val state: Option[CheckpointState] = Some(CheckpointState.OffsetValue(nextOffset.toLong))
          Some(((json, state), nextOffset))
        }
      }
    }.through(limitPages(limit, config))
  }

  private def linkHeaderPages(
      client: Client.RestClient,
      baseUri: Uri,
      params: Map[String, String],
      config: PaginationConfig,
      limit: Option[Int]
  ): Stream[IO, (Json, Option[CheckpointState])] = {
    val pageSizeParam = config.pageSizeParam

    val pageParams = pageSizeParam.map { psp =>
      val size = limit.map(l => math.min(l, config.maxPageSize)).getOrElse(config.maxPageSize)
      params + (psp -> size.toString)
    }.getOrElse(params)

    Stream.unfoldEval[IO, Option[Uri], (Json, Option[CheckpointState])](Some(baseUri)) {
      case None => IO.pure(None)
      case Some(uri) =>
        // For first request use pageParams, for subsequent use uri as-is (it includes params)
        val reqParams = if (uri == baseUri) pageParams else Map.empty[String, String]
        client.get(uri, reqParams).map { resp =>
          val nextLink = resp.headers.get("Link").flatMap(parseLinkHeader)
          val nextUri = nextLink.flatMap(link => Uri.fromString(link).toOption)
          Some(((resp.json, None), nextUri))
        }
    }.through(limitPages(limit, config))
  }

  private def parseLinkHeader(header: String): Option[String] = {
    // Parse: <url>; rel="next"
    header.split(",").map(_.trim).collectFirst {
      case s if s.contains("""rel="next"""") =>
        s.split(";").head.trim.stripPrefix("<").stripSuffix(">")
    }
  }

  /** Apply page limits. When a record limit is specified, compute the number of pages
    * needed. Always enforce max-pages as a safety net to prevent infinite pagination. */
  private def limitPages[A](limit: Option[Int], config: PaginationConfig): fs2.Pipe[IO, A, A] = {
    val safetyLimit = config.maxPages
    limit match {
      case None =>
        _.take(safetyLimit.toLong)
      case Some(l) =>
        // Take enough pages to cover the limit. With max page size, that's ceil(limit/pageSize) pages.
        val pagesForLimit = math.ceil(l.toDouble / config.maxPageSize).toInt.max(1)
        _.take(math.min(pagesForLimit, safetyLimit).toLong)
    }
  }
}

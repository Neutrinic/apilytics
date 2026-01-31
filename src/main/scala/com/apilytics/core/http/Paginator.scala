package com.apilytics.core.http

import cats.effect.IO
import com.apilytics.core.config.{PaginationConfig, PaginationStyle}
import fs2.Stream
import io.circe.Json
import io.circe.pointer.Pointer
import org.http4s.Uri

object Paginator {

  /** Stream of JSON pages from a paginated API endpoint. */
  def pages(
      client: Client.RestClient,
      baseUri: Uri,
      params: Map[String, String],
      pagination: PaginationConfig,
      limit: Option[Int] = None
  ): Stream[IO, Json] = {
    pagination.style match {
      case PaginationStyle.Cursor =>
        cursorPages(client, baseUri, params, pagination, limit)
      case PaginationStyle.Offset =>
        offsetPages(client, baseUri, params, pagination, limit)
      case PaginationStyle.LinkHeader =>
        linkHeaderPages(client, baseUri, params, pagination, limit)
      case PaginationStyle.None =>
        singlePage(client, baseUri, params)
    }
  }

  private def singlePage(
      client: Client.RestClient,
      baseUri: Uri,
      params: Map[String, String]
  ): Stream[IO, Json] = {
    Stream.eval(client.get(baseUri, params).map(_.json))
  }

  private def cursorPages(
      client: Client.RestClient,
      baseUri: Uri,
      params: Map[String, String],
      config: PaginationConfig,
      limit: Option[Int]
  ): Stream[IO, Json] = {
    val cursorPath = config.cursorPath
      .flatMap(p => Pointer.parse(p).toOption)
      .getOrElse(throw new IllegalArgumentException("Cursor pagination requires cursor-path"))
    val cursorParam = config.cursorParam.getOrElse("cursor")
    val pageSizeParam = config.pageSizeParam

    val pageParams = pageSizeParam.map { psp =>
      val size = limit.map(l => math.min(l, config.maxPageSize)).getOrElse(config.maxPageSize)
      params + (psp -> size.toString)
    }.getOrElse(params)

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
    Stream.eval(fetch(None)).flatMap {
      case None => Stream.empty
      case Some((firstPage, nextCursor)) =>
        Stream.emit(firstPage) ++ Stream.unfoldEval(nextCursor) {
          case None => IO.pure(None)
          case Some(cursor) =>
            fetch(Some(cursor)).map {
              case Some((page, next)) => Some((page, next))
              case None               => None
            }
        }
    }.through(limitPages(limit, config))
  }

  private def offsetPages(
      client: Client.RestClient,
      baseUri: Uri,
      params: Map[String, String],
      config: PaginationConfig,
      limit: Option[Int]
  ): Stream[IO, Json] = {
    val offsetParam = config.offsetParam.getOrElse("offset")
    val pageSizeParam = config.pageSizeParam.getOrElse("limit")
    val pageSize = limit.map(l => math.min(l, config.maxPageSize)).getOrElse(config.maxPageSize)

    Stream.unfoldEval[IO, Int, Json](0) { offset =>
      val reqParams = params + (offsetParam -> offset.toString) + (pageSizeParam -> pageSize.toString)
      client.get(baseUri, reqParams).map { resp =>
        // Stop if response is empty array or fewer items than page size
        // For now, always emit and let the limit pipe handle stopping
        Some((resp.json, offset + pageSize))
      }
    }.through(limitPages(limit, config))
  }

  private def linkHeaderPages(
      client: Client.RestClient,
      baseUri: Uri,
      params: Map[String, String],
      config: PaginationConfig,
      limit: Option[Int]
  ): Stream[IO, Json] = {
    val pageSizeParam = config.pageSizeParam

    val pageParams = pageSizeParam.map { psp =>
      val size = limit.map(l => math.min(l, config.maxPageSize)).getOrElse(config.maxPageSize)
      params + (psp -> size.toString)
    }.getOrElse(params)

    Stream.unfoldEval[IO, Option[Uri], Json](Some(baseUri)) {
      case None => IO.pure(None)
      case Some(uri) =>
        // For first request use pageParams, for subsequent use uri as-is (it includes params)
        val reqParams = if (uri == baseUri) pageParams else Map.empty[String, String]
        client.get(uri, reqParams).map { resp =>
          val nextLink = resp.headers.get("Link").flatMap(parseLinkHeader)
          val nextUri = nextLink.flatMap(link => Uri.fromString(link).toOption)
          Some((resp.json, nextUri))
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

  /** Limit total number of records across pages if limit is specified.
    * This limits by page count, not record count. Accurate record-level limiting
    * requires knowing data_path to count extracted records per page — handled in
    * RESTPartitionReader (Phase 2). */
  private def limitPages[A](limit: Option[Int], config: PaginationConfig): fs2.Pipe[IO, A, A] = {
    limit match {
      case None => identity
      case Some(l) =>
        // Take enough pages to cover the limit. With max page size, that's ceil(limit/pageSize) pages.
        val maxPages = math.ceil(l.toDouble / config.maxPageSize).toInt.max(1)
        _.take(maxPages)
    }
  }
}

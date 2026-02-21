package com.apilytics.core.http

import cats.effect.IO
import com.apilytics.core.checkpoint.CheckpointState
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig, PaginationConfig, PaginationStyle}
import io.circe.Json
import io.circe.parser._
import munit.CatsEffectSuite
import org.http4s.Uri

import scala.concurrent.duration._

class PaginatorSuite extends CatsEffectSuite {

  private val dummyAuth = AuthConfig(authType = AuthType.Bearer, token = Some("test"))
  private val dummyHttp = HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 1.second)

  private def mockClient(responses: List[(Json, Map[String, String])]): Client.RestClient = {
    var callIndex = 0
    new Client.RestClient(null, dummyHttp, dummyAuth, None, RateLimiter.unlimited, ResponseCache.disabled) {
      override def get(uri: Uri, params: Map[String, String]): IO[ApiResponse] = IO {
        val (json, headers) = responses(callIndex)
        callIndex += 1
        ApiResponse(json, 200, headers)
      }
    }
  }

  test("single page (no pagination) returns one result") {
    val json = parse("""[{"id": 1}]""").toOption.get
    val client = mockClient(List((json, Map.empty)))
    val config = PaginationConfig(style = PaginationStyle.None)

    Paginator.pages(client, Uri.unsafeFromString("http://test"), Map.empty, config)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 1)
        assertEquals(pages.head, json)
      }
  }

  test("cursor pagination follows cursor until empty") {
    val page1 = parse("""{"items": [1,2], "cursor": "abc"}""").toOption.get
    val page2 = parse("""{"items": [3], "cursor": ""}""").toOption.get
    val client = mockClient(List((page1, Map.empty), (page2, Map.empty)))

    val config = PaginationConfig(
      style = PaginationStyle.Cursor,
      cursorPath = Some("/cursor"),
      cursorParam = Some("cursor"),
      maxPageSize = 100
    )

    Paginator.pages(client, Uri.unsafeFromString("http://test"), Map.empty, config)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 2)
      }
  }

  test("link header pagination follows next link") {
    val page1 = parse("""{"data": [1]}""").toOption.get
    val page2 = parse("""{"data": [2]}""").toOption.get
    val client = mockClient(List(
      (page1, Map("Link" -> """<http://test?page=2>; rel="next"""")),
      (page2, Map.empty)
    ))

    val config = PaginationConfig(style = PaginationStyle.LinkHeader, maxPageSize = 100)

    Paginator.pages(client, Uri.unsafeFromString("http://test"), Map.empty, config)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 2)
      }
  }

  test("limit caps number of pages") {
    val page = parse("""{"items": [1]}""").toOption.get
    val client = mockClient(List.fill(10)((page, Map.empty)))

    val config = PaginationConfig(
      style = PaginationStyle.Offset,
      offsetParam = Some("offset"),
      pageSizeParam = Some("limit"),
      maxPageSize = 10
    )

    Paginator.pages(client, Uri.unsafeFromString("http://test"), Map.empty, config, limit = Some(5))
      .compile.toList.map { pages =>
        assertEquals(pages.size, 1)
      }
  }

  test("offset pagination stops on empty top-level array") {
    val page1 = parse("""[{"id": 1}]""").toOption.get
    val page2 = parse("""[{"id": 2}]""").toOption.get
    val empty = parse("""[]""").toOption.get
    val client = mockClient(List((page1, Map.empty), (page2, Map.empty), (empty, Map.empty)))

    val config = PaginationConfig(
      style = PaginationStyle.Offset,
      offsetParam = Some("offset"),
      pageSizeParam = Some("limit"),
      maxPageSize = 10
    )

    Paginator.pages(client, Uri.unsafeFromString("http://test"), Map.empty, config)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 2)
      }
  }

  test("offset pagination stops on empty results-path array") {
    val page1 = parse("""{"count": 2, "results": [{"id": 1}]}""").toOption.get
    val page2 = parse("""{"count": 2, "results": []}""").toOption.get
    val client = mockClient(List((page1, Map.empty), (page2, Map.empty)))

    val config = PaginationConfig(
      style = PaginationStyle.Offset,
      offsetParam = Some("offset"),
      pageSizeParam = Some("limit"),
      maxPageSize = 10,
      resultsPath = Some("/results")
    )

    Paginator.pages(client, Uri.unsafeFromString("http://test"), Map.empty, config)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 1)
      }
  }

  test("offset pagination respects max-pages safety limit") {
    val page = parse("""[{"id": 1}]""").toOption.get
    val client = mockClient(List.fill(20)((page, Map.empty)))

    val config = PaginationConfig(
      style = PaginationStyle.Offset,
      offsetParam = Some("offset"),
      pageSizeParam = Some("limit"),
      maxPageSize = 10,
      maxPages = 3
    )

    Paginator.pages(client, Uri.unsafeFromString("http://test"), Map.empty, config)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 3)
      }
  }

  test("cursor pagination respects max-pages safety limit") {
    val page = parse("""{"items": [1], "cursor": "next"}""").toOption.get
    val client = mockClient(List.fill(20)((page, Map.empty)))

    val config = PaginationConfig(
      style = PaginationStyle.Cursor,
      cursorPath = Some("/cursor"),
      cursorParam = Some("cursor"),
      maxPageSize = 100,
      maxPages = 2
    )

    Paginator.pages(client, Uri.unsafeFromString("http://test"), Map.empty, config)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 2)
      }
  }

  test("link header pagination respects max-pages safety limit") {
    val page = parse("""{"data": [1]}""").toOption.get
    val client = mockClient(List.fill(20)(
      (page, Map("Link" -> """<http://test?page=next>; rel="next""""))
    ))

    val config = PaginationConfig(
      style = PaginationStyle.LinkHeader,
      maxPageSize = 100,
      maxPages = 2
    )

    Paginator.pages(client, Uri.unsafeFromString("http://test"), Map.empty, config)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 2)
      }
  }

  // ============== CHECKPOINT STATE TESTS ==============

  test("cursor pagination emits checkpoint state with each page") {
    val page1 = parse("""{"items": [1,2], "cursor": "abc"}""").toOption.get
    val page2 = parse("""{"items": [3], "cursor": ""}""").toOption.get
    val client = mockClient(List((page1, Map.empty), (page2, Map.empty)))

    val config = PaginationConfig(
      style = PaginationStyle.Cursor,
      cursorPath = Some("/cursor"),
      cursorParam = Some("cursor"),
      maxPageSize = 100
    )

    Paginator.pagesWithState(client, Uri.unsafeFromString("http://test"), Map.empty, config)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 2)
        // First page has cursor "abc" -> checkpoint state should capture it
        assertEquals(pages(0)._2, Some(CheckpointState.CursorValue("abc")))
        // Second page has empty cursor -> state should fall back to previous cursor
        assert(pages(1)._2.isDefined)
      }
  }

  test("cursor pagination resumes from checkpoint state") {
    // Only return second page (simulates resuming from cursor "abc")
    val page2 = parse("""{"items": [3], "cursor": ""}""").toOption.get
    var capturedParams: Map[String, String] = Map.empty
    val client = new Client.RestClient(null, dummyHttp, dummyAuth, None, RateLimiter.unlimited, ResponseCache.disabled) {
      override def get(uri: Uri, params: Map[String, String]): IO[ApiResponse] = IO {
        capturedParams = params
        ApiResponse(page2, 200, Map.empty)
      }
    }

    val config = PaginationConfig(
      style = PaginationStyle.Cursor,
      cursorPath = Some("/cursor"),
      cursorParam = Some("cursor"),
      maxPageSize = 100
    )

    val startState = Some(CheckpointState.CursorValue("abc"))

    Paginator.pagesWithState(client, Uri.unsafeFromString("http://test"), Map.empty, config, startState = startState)
      .map(_._1)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 1)
        // Verify the cursor was passed as a query parameter
        assertEquals(capturedParams.get("cursor"), Some("abc"))
      }
  }

  test("offset pagination emits checkpoint state with each page") {
    val page1 = parse("""[{"id": 1}]""").toOption.get
    val page2 = parse("""[{"id": 2}]""").toOption.get
    val empty = parse("""[]""").toOption.get
    val client = mockClient(List((page1, Map.empty), (page2, Map.empty), (empty, Map.empty)))

    val config = PaginationConfig(
      style = PaginationStyle.Offset,
      offsetParam = Some("offset"),
      pageSizeParam = Some("limit"),
      maxPageSize = 10
    )

    Paginator.pagesWithState(client, Uri.unsafeFromString("http://test"), Map.empty, config)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 2)
        assertEquals(pages(0)._2, Some(CheckpointState.OffsetValue(10L)))
        assertEquals(pages(1)._2, Some(CheckpointState.OffsetValue(20L)))
      }
  }

  test("offset pagination resumes from checkpoint state") {
    val page = parse("""[{"id": 3}]""").toOption.get
    val empty = parse("""[]""").toOption.get
    var capturedParams: List[Map[String, String]] = Nil
    var callIndex = 0
    val responses = List(page, empty)
    val client = new Client.RestClient(null, dummyHttp, dummyAuth, None, RateLimiter.unlimited, ResponseCache.disabled) {
      override def get(uri: Uri, params: Map[String, String]): IO[ApiResponse] = IO {
        capturedParams = capturedParams :+ params
        val json = responses(callIndex)
        callIndex += 1
        ApiResponse(json, 200, Map.empty)
      }
    }

    val config = PaginationConfig(
      style = PaginationStyle.Offset,
      offsetParam = Some("offset"),
      pageSizeParam = Some("limit"),
      maxPageSize = 10
    )

    val startState = Some(CheckpointState.OffsetValue(20L))

    Paginator.pagesWithState(client, Uri.unsafeFromString("http://test"), Map.empty, config, startState = startState)
      .map(_._1)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 1)
        // Verify offset started from 20, not 0
        assertEquals(capturedParams.head.get("offset"), Some("20"))
      }
  }

  test("single page (no pagination) returns None checkpoint state") {
    val json = parse("""[{"id": 1}]""").toOption.get
    val client = mockClient(List((json, Map.empty)))
    val config = PaginationConfig(style = PaginationStyle.None)

    Paginator.pagesWithState(client, Uri.unsafeFromString("http://test"), Map.empty, config)
      .compile.toList.map { pages =>
        assertEquals(pages.size, 1)
        assertEquals(pages.head._2, None)
      }
  }
}

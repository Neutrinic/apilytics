package com.apilytics.core.http

import cats.effect.IO
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
    new Client.RestClient(null, dummyHttp, dummyAuth, None) {
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
}

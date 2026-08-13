package com.apilytics.core.rest

import cats.effect.unsafe.implicits.global
import com.apilytics.core.checkpoint.CheckpointState
import com.apilytics.core.config._
import com.apilytics.core.openapi.Endpoint
import com.apilytics.core.schema.SourceSchema
import com.apilytics.core.source.{ReadRequest, RecordPage, RecordSource, SourceHandle}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite

import scala.concurrent.duration._

/** REST behind the protocol-neutral read interface (#191).
  *
  * These exercise `RecordSource` / `RecordSession` only — no HTTP client, paginator or
  * OpenAPI type appears in the test body, which is the property the seam exists to
  * provide. If a future protocol satisfies these same shapes, the Spark layer can read
  * from it unchanged.
  */
class RestSourceSuite extends FunSuite {

  private var server: WireMockServer = _

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
  }

  override def afterEach(context: AfterEach): Unit = server.stop()

  private val endpoint = Endpoint(
    path = "/items",
    operationId = Some("listItems"),
    responseSchema = SourceSchema.ObjectType(Map("id" -> SourceSchema.IntegerType())),
    queryParams = Nil
  )

  private def config(
      pagination: PaginationConfig = PaginationConfig(),
      format: ResponseFormat = ResponseFormat.Json
  ) = SourceConfig(
    openapi = "test.yaml",
    auth = AuthConfig(
      authType = AuthType.Header,
      headerName = Some("Accept"),
      headerValue = Some("application/json")
    ),
    pagination = pagination,
    http = HttpConfig(
      maxRetries = 0,
      maxBackoff = 1.second,
      timeout = 5.seconds,
      responseFormat = format
    )
  )

  private def handle(dataPath: Option[String]) = RestHandle(
    endpoint = endpoint,
    baseUrl = s"http://localhost:${server.port()}",
    tableConfig = dataPath.map(dp => TableConfig(endpoint = "/items", dataPath = Some(dp)))
  )

  /** Read every page through the neutral interface. */
  private def readAll(source: RecordSource, request: ReadRequest): List[RecordPage] =
    source.session.use(_.pages(request).compile.toList).unsafeRunSync()

  test("reads records through the neutral interface") {
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .willReturn(okJson("""{"results": [{"id": 1}, {"id": 2}]}"""))
    )

    val pages = readAll(new RestSource(config()), ReadRequest(handle(Some("/results"))))

    assertEquals(pages.flatMap(_.records).size, 2)
    assertEquals(
      pages.flatMap(_.records).flatMap(_.hcursor.get[Int]("id").toOption),
      List(1, 2)
    )
  }

  test("data-path extraction is the source's concern, not the caller's") {
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .willReturn(okJson("""{"payload": {"rows": [{"id": 7}]}}"""))
    )

    val pages = readAll(new RestSource(config()), ReadRequest(handle(Some("/payload/rows"))))

    assertEquals(pages.flatMap(_.records).size, 1)
    assertEquals(pages.flatMap(_.records).head.hcursor.get[Int]("id").toOption, Some(7))
  }

  test("pushed-down params reach the request") {
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("status", equalTo("active"))
        .willReturn(okJson("""{"results": [{"id": 1}]}"""))
    )

    val pages = readAll(
      new RestSource(config()),
      ReadRequest(handle(Some("/results")), params = Map("status" -> "active"))
    )

    assertEquals(pages.flatMap(_.records).size, 1)
    server.verify(getRequestedFor(urlPathEqualTo("/items")).withQueryParam("status", equalTo("active")))
  }

  test("pagination is followed and checkpoint state is surfaced per page") {
    server.stubFor(
      get(urlPathEqualTo("/items")).withQueryParam("offset", equalTo("0"))
        .willReturn(okJson("""{"results": [{"id": 1}, {"id": 2}]}"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/items")).withQueryParam("offset", equalTo("2"))
        .willReturn(okJson("""{"results": []}"""))
    )

    val pages = readAll(
      new RestSource(config(PaginationConfig(
        style = PaginationStyle.Offset,
        offsetParam = Some("offset"),
        pageSizeParam = Some("limit"),
        maxPageSize = 2,
        resultsPath = Some("/results")
      ))),
      ReadRequest(handle(Some("/results")))
    )

    assertEquals(pages.flatMap(_.records).size, 2)
    assert(pages.exists(_.state.isDefined), "offset pagination should report checkpoint state")
  }

  test("NDJSON records arrive without data-path extraction") {
    server.stubFor(
      get(urlPathEqualTo("/items")).willReturn(
        aResponse().withStatus(200)
          .withHeader("Content-Type", "application/x-ndjson")
          .withBody("{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}\n")
      )
    )

    // dataPath is set but must be ignored: every NDJSON line is already a record.
    val pages = readAll(
      new RestSource(config(format = ResponseFormat.NDJSON)),
      ReadRequest(handle(Some("/results")))
    )

    assertEquals(pages.flatMap(_.records).size, 3)
  }

  test("a handle from another protocol fails loudly") {
    final case class ForeignHandle() extends SourceHandle

    val ex = intercept[IllegalArgumentException] {
      readAll(new RestSource(config()), ReadRequest(ForeignHandle()))
    }
    assert(ex.getMessage.contains("cannot read a ForeignHandle"), ex.getMessage)
  }

  test("resuming from checkpoint state does not re-read from the start") {
    server.stubFor(
      get(urlPathEqualTo("/items")).withQueryParam("offset", equalTo("4"))
        .willReturn(okJson("""{"results": [{"id": 5}]}"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/items")).withQueryParam("offset", equalTo("5"))
        .willReturn(okJson("""{"results": []}"""))
    )

    val pages = readAll(
      new RestSource(config(PaginationConfig(
        style = PaginationStyle.Offset,
        offsetParam = Some("offset"),
        pageSizeParam = Some("limit"),
        maxPageSize = 1,
        resultsPath = Some("/results")
      ))),
      ReadRequest(handle(Some("/results")), startState = Some(CheckpointState.OffsetValue(4)))
    )

    assertEquals(pages.flatMap(_.records).flatMap(_.hcursor.get[Int]("id").toOption), List(5))
    server.verify(0, getRequestedFor(urlPathEqualTo("/items")).withQueryParam("offset", equalTo("0")))
  }
}

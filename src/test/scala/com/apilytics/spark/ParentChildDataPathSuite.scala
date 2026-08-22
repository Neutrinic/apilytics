package com.apilytics.spark

import com.apilytics.core.config._
import com.apilytics.core.rest.RestHandle
import com.apilytics.core.schema.{SchemaMapper, SourceSchema}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite

import scala.concurrent.duration._

/** Parent-child joins where the child response wraps its records (#217).
  *
  * The existing parent-child tests use child endpoints returning a bare top-level array,
  * which sidesteps two things a real API runs into: the child needing `data-path` to reach
  * its records, and the child not paginating the way the source-level config expects. Both
  * failed silently — zero rows, no error — until an example config exercised them.
  */
class ParentChildDataPathSuite extends FunSuite {

  private var server: WireMockServer = _

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
  }

  override def afterEach(context: AfterEach): Unit = server.stop()

  /** Child records live under `/pokemon`, mirroring PokéAPI's type-detail endpoint. */
  private val childRecordSchema = SourceSchema.ObjectType(
    Map(
      "slot"    -> SourceSchema.IntegerType(),
      "pokemon" -> SourceSchema.ObjectType(Map("name" -> SourceSchema.StringType()))
    )
  )

  /** Source-level pagination, tuned for the *list* endpoints as a real config would be. */
  private val listPagination = PaginationConfig(
    style = PaginationStyle.Offset,
    offsetParam = Some("offset"),
    pageSizeParam = Some("limit"),
    maxPageSize = 100,
    resultsPath = Some("/results")
  )

  private def stubEndpoints(): Unit = {
    // Offset-aware, so the parent's pagination terminates the way a real list endpoint
    // would. A stub that ignores `offset` never yields an empty page and runs to
    // `max-pages`, which is a property of the fixture rather than of the connector.
    server.stubFor(
      get(urlPathEqualTo("/types")).withQueryParam("offset", equalTo("0"))
        .willReturn(okJson("""{"results": [{"name": "fire"}]}"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/types")).withQueryParam("offset", equalTo("100"))
        .willReturn(okJson("""{"results": []}"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/type/fire")).willReturn(
        okJson("""{"id": 10, "name": "fire", "pokemon": [{"slot": 1, "pokemon": {"name": "charmander"}}]}""")
      )
    )
  }

  private def readRows(childPagination: Option[PaginationConfig]): Int = {
    val base = s"http://localhost:${server.port()}"

    val sourceConfig = SourceConfig(
      openapi = "test.yaml",
      auth = AuthConfig(
        authType = AuthType.Header,
        headerName = Some("Accept"),
        headerValue = Some("application/json")
      ),
      pagination = listPagination,
      http = HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 10.seconds)
    )

    val childTableConfig = TableConfig(
      endpoint = "/type/{type_name}",
      dataPath = Some("/pokemon"),
      pagination = childPagination
    )

    val partition = ParentChildInputPartition(
      childEndpointTemplate = "/type/{type_name}",
      // The parent is a list endpoint, so it keeps the source-level pagination.
      parentHandle = RestHandle(
        "/types",
        base,
        Some(TableConfig(endpoint = "/types", dataPath = Some("/results")))
      ),
      childResponseSchema = childRecordSchema,
      parentKey = "name",
      pathParamName = "type_name",
      parentKeyColumn = "_parent_type_name",
      tableConfig = childTableConfig,
      sourceConfig = sourceConfig,
      baseUrl = base,
      arrowSchemaJson = SchemaMapper.toArrowSchema(childRecordSchema).toJson,
      pushedParams = Map.empty,
      pushedLimit = None
    )

    val reader = new ParentChildColumnarPartitionReader(partition)
    try {
      var rows = 0
      while (reader.next()) rows += reader.get().numRows()
      rows
    } finally reader.close()
  }

  test("child records are reachable through data-path") {
    stubEndpoints()

    // Without the per-table override this returned 0: the paginator inherited the list
    // endpoint's `results-path`, failed to find `/results` in the detail response, and
    // concluded the page was empty.
    val rows = readRows(childPagination = Some(PaginationConfig()))

    assertEquals(rows, 1, "child records under data-path should be read")
    server.verify(getRequestedFor(urlPathEqualTo("/type/fire")))
  }

  test("inheriting list pagination on a detail endpoint yields nothing") {
    stubEndpoints()

    // Pins the failure mode itself, so the reason for the override cannot be forgotten:
    // source-level pagination applied to a detail endpoint silently produces no rows.
    val rows = readRows(childPagination = None)

    assertEquals(
      rows,
      0,
      "documents why per-table pagination exists — if this ever returns rows, the override may no longer be needed"
    )
  }

  test("a table's pagination overrides the source's") {
    val cfg = Loader.load(com.typesafe.config.ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |pagination { style = offset, offset-param = "offset", results-path = "/results" }
        |tables {
        |  detail { endpoint = "/x/{id}", data-path = "/items", pagination { style = none } }
        |  list   { endpoint = "/xs", data-path = "/results" }
        |}
        |""".stripMargin))

    assertEquals(cfg.pagination.style, PaginationStyle.Offset)
    assertEquals(cfg.tables("detail").pagination.map(_.style), Some(PaginationStyle.None))
    assertEquals(cfg.tables("list").pagination, None, "tables without an override inherit the source")
  }
}

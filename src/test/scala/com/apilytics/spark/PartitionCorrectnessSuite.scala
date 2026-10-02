package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

/** Partitioned and offset-paginated reads return each record exactly once (#292). */
class PartitionCorrectnessSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var dir: Path = _

  private val spec =
    """openapi: 3.0.0
      |info: { title: items, version: "1" }
      |paths:
      |  /items:
      |    get:
      |      parameters:
      |        - { name: offset, in: query, schema: { type: integer } }
      |        - { name: limit, in: query, schema: { type: integer } }
      |        - { name: kind, in: query, schema: { type: string } }
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/json:
      |              schema:
      |                type: object
      |                properties:
      |                  results:
      |                    type: array
      |                    items: { type: object, properties: { id: { type: integer } } }
      |""".stripMargin

  private def page(ids: Range): String = ids.map(i => s"""{"id": $i}""").mkString("""{"results": [""", ",", "]}")

  /** An API holding records 0 until `total`, serving `[offset, offset + min(limit, cap))`,
    * or a fixed `ignoring` records whatever the limit, like an API that ignores it.
    */
  private def stubOffsetApi(total: Int, cap: Int = Int.MaxValue, ignoring: Option[Int] = None): Unit =
    for (offset <- 0 to total by 50; limit <- Seq(50, 100)) {
      val served = ignoring.getOrElse(math.min(limit, cap))
      server.stubFor(get(urlPathEqualTo("/items"))
        .withQueryParam("offset", equalTo(offset.toString)).withQueryParam("limit", equalTo(limit.toString))
        .willReturn(okJson(page(offset until math.min(offset + served, total)))))
    }

  private def start(tableBody: String): Unit = {
    Files.writeString(dir.resolve("items.yaml"), spec)
    Files.writeString(
      dir.resolve("items.conf"),
      s"""openapi = "items.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |http { max-retries = 0, max-backoff = "0 seconds" }
         |pagination { style = offset, offset-param = "offset", page-size-param = "limit", max-page-size = 100, results-path = "/results" }
         |tables {
         |  items {
         |    endpoint = "/items"
         |    data-path = "/results"
         |    $tableBody
         |  }
         |}
         |""".stripMargin
    )
    spark = SparkSession.builder().master("local[2]").appName("PartitionCorrectnessSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", dir.resolve("items.conf").toAbsolutePath.toString)
      .getOrCreate()
  }

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    dir = Files.createTempDirectory("apilytics-partitions")
  }

  override def afterEach(context: AfterEach): Unit =
    try {
      if (spark != null) spark.stop()
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      try { if (server != null) server.stop() }
      finally if (dir != null) Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    }

  private def ids(sql: String): List[Long] =
    spark.sql(sql).collect().map(_.getAs[Number](0).longValue).toList.sorted

  test("offset windows that aren't a multiple of the page size don't overlap") {
    // Two windows of 150 with 100-record pages: each must stop at its boundary, not read a
    // second full page into the next window. Before the fix: 350 rows, 50 of them twice.
    stubOffsetApi(total = 300)
    start("""partition { type = "offset", size = 150, count = 2 }""")

    assertEquals(ids("SELECT id FROM api.default.items"), (0L until 300L).toList)
  }

  test("an offset window is filled even when the API serves smaller pages than asked") {
    // The API caps pages at 50 though asked for 100: a window must keep reading until it
    // holds its records, rather than stop after the pages it expected to need.
    stubOffsetApi(total = 300, cap = 50)
    start("""partition { type = "offset", size = 150, count = 2 }""")

    assertEquals(ids("SELECT id FROM api.default.items"), (0L until 300L).toList)
  }

  test("an offset window holds exactly its records even when the API ignores the page size") {
    // Asked for 50, the API sends 100: the page is trimmed to what was asked for, so the
    // window doesn't spill into the next one.
    stubOffsetApi(total = 300, ignoring = Some(100))
    start("""partition { type = "offset", size = 150, count = 2 }""")

    assertEquals(ids("SELECT id FROM api.default.items"), (0L until 300L).toList)
  }

  test("an offset checkpoint resumes after the records actually read") {
    // Two records with 100-record pages: the next run must resume at 2, not 100, or a
    // record appended at 2 is never read.
    server.stubFor(get(urlPathEqualTo("/items")).withQueryParam("offset", equalTo("0"))
      .willReturn(okJson(page(0 until 2))))
    server.stubFor(get(urlPathEqualTo("/items")).withQueryParam("offset", equalTo("2"))
      .willReturn(okJson(page(0 until 0))))
    val checkpoints = dir.resolve("ck").toAbsolutePath.toString.replace("\\", "/")
    start(s"""checkpoint { enabled = true, path = "$checkpoints", mode = offset }""")

    assertEquals(ids("SELECT id FROM api.default.items"), List(0L, 1L))
    val saved = Files.readString(dir.resolve("ck").resolve("items.checkpoint.json"))
    assert(saved.contains("\"value\":2"), saved)
  }

  test("enum partitions keep a filter already pushed to their parameter") {
    // `id = 1` is pushed to `kind`; the enum must not override it with each of its values,
    // because Spark has already dropped the predicate from its plan.
    server.stubFor(get(urlPathEqualTo("/items")).withQueryParam("kind", equalTo("1"))
      .willReturn(okJson(page(1 to 1))))
    server.stubFor(get(urlPathEqualTo("/items")).withQueryParam("kind", equalTo("2"))
      .willReturn(okJson(page(2 to 2))))
    start("""pagination { style = none }
            |    filters = [ { param = "kind", column = "id", operators = ["eq"] } ]
            |    partition { type = "enum", param = "kind", values = ["1", "2"] }""".stripMargin)

    assertEquals(ids("SELECT id FROM api.default.items WHERE id = 1"), List(1L))
    assertEquals(ids("SELECT id FROM api.default.items"), List(1L, 2L))
  }
}

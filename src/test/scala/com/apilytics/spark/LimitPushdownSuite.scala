package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

/** A pushed LIMIT never stops a read before it has the rows the query asked for (#298). */
class LimitPushdownSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var dir: Path = _

  private val spec =
    """openapi: 3.0.0
      |info: { title: limits, version: "1" }
      |paths:
      |  /events:
      |    get:
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/json:
      |              schema:
      |                type: object
      |                properties:
      |                  next: { type: string }
      |                  items:
      |                    type: array
      |                    items: { type: object, properties: { id: { type: integer } } }
      |  /parents:
      |    get:
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
      |                    items:
      |                      type: object
      |                      properties:
      |                        id: { type: integer }
      |                        tags: { type: array, items: { type: string } }
      |""".stripMargin

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    dir = Files.createTempDirectory("apilytics-limit")
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

  private def start(config: String): Unit = {
    Files.writeString(dir.resolve("limits.yaml"), spec)
    Files.writeString(dir.resolve("limits.conf"),
      s"""openapi = "limits.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |http { max-retries = 0, max-backoff = "0 seconds" }
         |$config
         |""".stripMargin)
    spark = SparkSession.builder().master("local[1]").appName("LimitPushdownSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", dir.resolve("limits.conf").toAbsolutePath.toString)
      .getOrCreate()
  }

  test("LIMIT keeps reading cursor pages until it has its rows") {
    // One record per page, though up to 100 are allowed: assuming full pages stopped after
    // one page and returned one row for LIMIT 2.
    server.stubFor(get(urlPathEqualTo("/events")).withQueryParam("cursor", absent())
      .willReturn(okJson("""{"items": [{"id": 1}], "next": "c2"}""")))
    server.stubFor(get(urlPathEqualTo("/events")).withQueryParam("cursor", equalTo("c2"))
      .willReturn(okJson("""{"items": [{"id": 2}], "next": "c3"}""")))
    server.stubFor(get(urlPathEqualTo("/events")).withQueryParam("cursor", equalTo("c3"))
      .willReturn(okJson("""{"items": [{"id": 3}], "next": ""}""")))
    start("""pagination { style = cursor, cursor-path = "/next", cursor-param = "cursor", max-page-size = 100 }
            |tables { events { endpoint = "/events", data-path = "/items" } }""".stripMargin)

    val ids = spark.sql("SELECT id FROM api.default.events LIMIT 2").collect()
      .map(_.getAs[Number](0).longValue).toList.sorted
    assertEquals(ids, List(1L, 2L))
  }

  test("a LIMIT that stops the read early doesn't fail the query with a memory leak (#307)") {
    // Spark closes the reader once it has its row, while the producer still holds batches.
    // A batch converted but not yet queued was never released, and in local mode the query
    // failed with "Memory was leaked by query". Many small batches, one in flight at a time,
    // make that the usual state at close.
    val pages = 10
    (0 until pages).foreach { p =>
      val items = (0 until 20).map(i => s"""{"id": ${p * 20 + i}}""").mkString(",")
      val cursor = if (p == 0) absent() else equalTo(s"c$p")
      val next = if (p + 1 < pages) s"c${p + 1}" else ""
      server.stubFor(get(urlPathEqualTo("/events")).withQueryParam("cursor", cursor)
        .willReturn(okJson(s"""{"items": [$items], "next": "$next"}""")))
    }
    start("""pagination { style = cursor, cursor-path = "/next", cursor-param = "cursor", max-page-size = 20 }
            |schema { arrow-batch-size = 2, prefetch-batches = 1 }
            |tables { events { endpoint = "/events", data-path = "/items" } }""".stripMargin)

    (1 to 30).foreach { i =>
      val rows =
        try spark.sql("SELECT id FROM api.default.events LIMIT 1").collect()
        catch { case e: Throwable => fail(s"query $i failed: $e", e) }
      assertEquals(rows.length, 1)
    }
  }

  test("LIMIT on an exploded table counts output rows, not parent records") {
    // The first parent's array is empty. Fetching only one parent for LIMIT 1 returned
    // no rows at all.
    server.stubFor(get(urlPathEqualTo("/parents")).withQueryParam("offset", equalTo("0"))
      .withQueryParam("limit", equalTo("1"))
      .willReturn(okJson("""{"results": [{"id": 1, "tags": []}]}""")))
    server.stubFor(get(urlPathEqualTo("/parents")).withQueryParam("offset", equalTo("0"))
      .withQueryParam("limit", equalTo("100"))
      .willReturn(okJson("""{"results": [{"id": 1, "tags": []}, {"id": 2, "tags": ["x"]}]}""")))
    server.stubFor(get(urlPathEqualTo("/parents")).withQueryParam("offset", equalTo("2"))
      .willReturn(okJson("""{"results": []}""")))
    start("""pagination { style = offset, offset-param = "offset", page-size-param = "limit", max-page-size = 100, results-path = "/results" }
            |schema { array-handling = both }
            |tables { parents { endpoint = "/parents", data-path = "/results" } }""".stripMargin)

    val rows = spark.sql("SELECT tags FROM api.default.parents_tags LIMIT 1").collect()
    assertEquals(rows.map(_.getString(0)).toList, List("x"))
  }
}

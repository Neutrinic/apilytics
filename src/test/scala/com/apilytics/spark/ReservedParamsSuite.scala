package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

/** A filter on a parameter pagination sends is kept for Spark, not pushed (#335).
  *
  * It used to be rejected at load, which refused configs that load on 0.8.0, such as a copy
  * of its PokeAPI example. Pushed, its value would be replaced by pagination's, and the rows
  * wouldn't match the WHERE.
  */
class ReservedParamsSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var dir: Path = _

  private val spec =
    """openapi: 3.0.0
      |info: { title: items, version: "1" }
      |paths:
      |  /items:
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
      |                    items: { type: object, properties: { id: { type: integer }, size: { type: integer } } }
      |""".stripMargin

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    dir = Files.createTempDirectory("apilytics-reserved")
    Files.writeString(dir.resolve("items.yaml"), spec)
    Files.writeString(dir.resolve("items.conf"),
      s"""openapi = "items.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |http { max-retries = 0, max-backoff = "0 seconds" }
         |pagination { style = offset, offset-param = "offset", page-size-param = "limit", max-page-size = 100, results-path = "/results" }
         |tables { items { endpoint = "/items", data-path = "/results",
         |  filters = [ { param = "limit", column = "size", operators = ["eq"] } ] } }
         |""".stripMargin)
    spark = SparkSession.builder().master("local[1]").appName("ReservedParamsSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", dir.resolve("items.conf").toAbsolutePath.toString)
      .getOrCreate()
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

  test("a filter on pagination's own parameter is applied by Spark, not sent") {
    server.stubFor(get(urlPathEqualTo("/items")).withQueryParam("offset", equalTo("0"))
      .willReturn(okJson("""{"results": [{"id": 1, "size": 1}, {"id": 2, "size": 2}, {"id": 3, "size": 2}]}""")))
    server.stubFor(get(urlPathEqualTo("/items")).withQueryParam("offset", equalTo("3"))
      .willReturn(okJson("""{"results": []}""")))

    val ids = spark.sql("SELECT id FROM api.default.items WHERE size = 2").collect()
      .map(_.getAs[Number](0).longValue).toList.sorted

    assertEquals(ids, List(2L, 3L))
    // Every request carries pagination's page size, never the filter's value.
    val limits = server.getAllServeEvents.asScala.map(_.getRequest.queryParameter("limit").firstValue()).toSet
    assertEquals(limits, Set("100"))
  }
}

package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

/** A row object that allows extra fields keeps its declared columns (#341).
  *
  * With `additionalProperties: true` on the row, the parser used to make the row a VARIANT, so
  * `data-path` couldn't reach a row schema and the table collapsed to the response wrapper: one
  * `results` column, NULL on every row.
  */
class AdditionalPropertiesSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var dir: Path = _

  private val spec =
    """openapi: 3.0.0
      |info: { title: trips, version: "1" }
      |paths:
      |  /trips:
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
      |                        fare: { type: number }
      |                        tags:
      |                          type: object
      |                          additionalProperties: { type: string }
      |                      additionalProperties: true
      |""".stripMargin

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    dir = Files.createTempDirectory("apilytics-additional-properties")
    Files.writeString(dir.resolve("trips.yaml"), spec)
    Files.writeString(dir.resolve("trips.conf"),
      s"""openapi = "trips.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |http { max-retries = 0, max-backoff = "0 seconds" }
         |tables { trips { endpoint = "/trips", data-path = "/results" } }
         |""".stripMargin)
    spark = SparkSession.builder().master("local[1]").appName("AdditionalPropertiesSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", dir.resolve("trips.conf").toAbsolutePath.toString)
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

  test("a row that allows extra fields keeps its declared columns, and skips the extras") {
    // `tip` isn't declared, so it's skipped. `tags` declares no properties, only a value type:
    // a map, which stays one JSON column.
    server.stubFor(get(urlPathEqualTo("/trips")).willReturn(okJson(
      """{"results": [{"id": 1, "fare": 12.5, "tip": 2.0, "tags": {"zone": "a"}},
        |             {"id": 2, "fare": 7.0}]}""".stripMargin
    )))

    val df = spark.table("api.default.trips")
    assertEquals(df.columns.toList.sorted, List("fare", "id", "tags"))

    val rows = df.orderBy("id").collect().toList
    assertEquals(rows.map(r => (r.getAs[Number]("id").longValue, r.getAs[Double]("fare"))), List(1L -> 12.5, 2L -> 7.0))
    assertEquals(Option(rows.head.getAs[String]("tags")), Some("""{"zone":"a"}"""))
  }
}

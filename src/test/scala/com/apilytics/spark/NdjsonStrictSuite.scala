package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

/** An NDJSON source takes strict-mode columns from the record its spec describes (#345).
  *
  * Only JSON media types used to supply columns, so an endpoint the spec gives as
  * `application/x-ndjson` was refused in strict mode even when it described the records.
  */
class NdjsonStrictSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var dir: Path = _

  private val spec =
    """openapi: 3.0.0
      |info: { title: exports, version: "1" }
      |paths:
      |  /export:
      |    get:
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/x-ndjson:
      |              schema:
      |                type: object
      |                properties:
      |                  id: { type: integer }
      |                  fare: { type: number }
      |                  at: { type: string, format: date-time }
      |  /raw:
      |    get:
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/x-ndjson:
      |              schema: { type: string }
      |""".stripMargin

  private val body =
    """{"id": 1, "fare": 12.5, "at": "2024-01-02T03:04:05Z", "extra": true}
      |{"id": 2, "fare": 7.0, "at": "2024-01-02T04:00:00Z"}
      |""".stripMargin

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    for (path <- List("/export", "/raw"))
      server.stubFor(get(urlPathEqualTo(path)).willReturn(
        aResponse().withHeader("Content-Type", "application/x-ndjson").withBody(body)
      ))
    dir = Files.createTempDirectory("apilytics-ndjson-strict")
    Files.writeString(dir.resolve("exports.yaml"), spec)
    def conf(format: String) =
      s"""openapi = "exports.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |pagination { style = none }
         |http { response-format = $format, max-retries = 0, max-backoff = "0 seconds" }
         |tables {
         |  export { endpoint = "/export" }
         |  raw    { endpoint = "/raw" }
         |}
         |""".stripMargin
    Files.writeString(dir.resolve("ndjson.conf"), conf("ndjson"))
    Files.writeString(dir.resolve("json.conf"), conf("json"))
    spark = SparkSession.builder().master("local[1]").appName("NdjsonStrictSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", dir.resolve("ndjson.conf").toAbsolutePath.toString)
      .config("spark.sql.catalog.apijson", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.apijson.config", dir.resolve("json.conf").toAbsolutePath.toString)
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

  private def failure(table: String): String = {
    val ex = intercept[Exception](spark.table(table))
    Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")
  }

  test("an NDJSON source gets typed columns from the record schema, in strict mode") {
    val df = spark.table("api.default.export")
    assertEquals(df.columns.toList.sorted, List("at", "fare", "id"))

    val rows = df.orderBy("id").collect().toList
    assertEquals(rows.map(r => (r.getAs[Number]("id").longValue, r.getAs[Double]("fare"))), List(1L -> 12.5, 2L -> 7.0))
    assertEquals(rows.head.getAs[java.sql.Timestamp]("at").toInstant, java.time.Instant.parse("2024-01-02T03:04:05Z"))
  }

  test("an NDJSON response with no record schema is still refused, saying so") {
    val msg = failure("api.default.raw")
    assert(msg.contains("'raw'") && msg.contains("no record schema") && msg.contains("variant"), msg)
  }

  test("a JSON source still refuses an NDJSON-only endpoint, whose body it can't read") {
    val msg = failure("apijson.default.export")
    assert(msg.contains("'export'") && msg.contains("none of them JSON"), msg)
  }
}

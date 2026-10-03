package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.{IntegerType, LongType}

import java.nio.file.{Files, Path}

/** Integer width, value conversion and the two metrics that count it, through Spark (#308, #309). */
class ConversionMetricsSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var dir: Path = _

  private val spec =
    """openapi: 3.0.0
      |info: { title: values, version: "1" }
      |paths:
      |  /items:
      |    get:
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/json:
      |              schema:
      |                type: array
      |                items:
      |                  type: object
      |                  properties:
      |                    id: { type: integer }
      |                    small: { type: integer, format: int32 }
      |                    flag: { type: boolean }
      |                    updated_at: { type: string, format: date-time }
      |""".stripMargin

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    dir = Files.createTempDirectory("apilytics-values")
    Files.writeString(dir.resolve("values.yaml"), spec)
    Files.writeString(dir.resolve("values.conf"),
      s"""openapi = "values.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |http { max-retries = 0, max-backoff = "0 seconds" }
         |tables { items { endpoint = "/items" } }
         |""".stripMargin)
    // AQE off, so the executed plan is the scan itself rather than a wrapper around it.
    spark = SparkSession.builder().master("local[1]").appName("ConversionMetricsSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", dir.resolve("values.conf").toAbsolutePath.toString)
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

  test("an unformatted integer is a bigint and keeps values past 32 bits (#308)") {
    server.stubFor(get(urlPathEqualTo("/items"))
      .willReturn(okJson("""[{"id": 3000000000, "small": 1}]""")))

    val df = spark.table("api.default.items")
    assertEquals(df.schema("id").dataType, LongType)
    assertEquals(df.schema("small").dataType, IntegerType, "an explicit int32 stays an int")
    assertEquals(df.select("id").collect().map(_.getLong(0)).toList, List(3000000000L))
  }

  test("converted and NULLed values are counted on the scan in the SQL tab (#309)") {
    server.stubFor(get(urlPathEqualTo("/items"))
      .willReturn(okJson(
        """[
          |  {"id": "42",  "small": 1,          "flag": "TRUE", "updated_at": "2024-01-02 03:04:05"},
          |  {"id": 7,     "small": 3000000000, "flag": true,   "updated_at": ""},
          |  {"id": "4.2", "small": 2,          "flag": "yes",  "updated_at": "2024-01-02T03:04:05Z"}
          |]""".stripMargin)))

    val df   = spark.sql("SELECT id, small, flag, updated_at FROM api.default.items")
    val rows = df.collect().toList

    // The read doesn't fail: what has one reading is converted, the rest is NULL.
    assertEquals(rows.map(r => Option(r.get(0))), List(Some(42L), Some(7L), None))
    assertEquals(rows.map(r => Option(r.get(1))), List(Some(1), None, Some(2)))
    assertEquals(rows.map(r => Option(r.get(2))), List(Some(true), Some(true), None))
    assert(rows.head.get(3) != null && rows(1).get(3) == null, rows.map(_.get(3)).toString)

    val scan = df.queryExecution.executedPlan.collectFirst { case s: BatchScanExec => s }
      .getOrElse(fail("no BatchScanExec in the plan"))
    // "42", "TRUE" and the space-separated timestamp were converted; "4.2", the int32
    // overflow, "yes" and the empty timestamp became NULL.
    assertEquals(scan.metrics(ConversionMetrics.ConvertedName).value, 3L)
    assertEquals(scan.metrics(ConversionMetrics.NulledName).value, 4L)
    assertEquals(scan.metrics(ConversionMetrics.NulledName).name, Some("values replaced with NULL"))
  }
}

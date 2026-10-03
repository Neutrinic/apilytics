package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

/** Response content types other than an exact `application/json` (#319). */
class ResponseContentTypeSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var dir: Path = _

  private val spec =
    """openapi: 3.0.0
      |info: { title: reports, version: "1" }
      |paths:
      |  /items:
      |    get:
      |      operationId: listing
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/json; charset=utf-8:
      |              schema:
      |                type: array
      |                items: { type: object, properties: { id: { type: integer }, name: { type: string } } }
      |  /report:
      |    get:
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            text/csv:
      |              schema: { type: string }
      |  /reports/latest:
      |    get:
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            text/csv:
      |              schema: { type: string }
      |  /reports/{id}:
      |    get:
      |      parameters:
      |        - { name: id, in: path, required: true, schema: { type: string } }
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/json:
      |              schema: { type: object, properties: { id: { type: string } } }
      |  /raw:
      |    get:
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/json: {}
      |""".stripMargin

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    dir = Files.createTempDirectory("apilytics-content-type")
    Files.writeString(dir.resolve("reports.yaml"), spec)
    def conf(mode: String) =
      s"""openapi = "reports.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |http { max-retries = 0, max-backoff = "0 seconds" }
         |schema { mode = $mode }
         |tables {
         |  items  { endpoint = "/items" }
         |  report { endpoint = "/report" }
         |  listing { endpoint = "/report" }
         |  raw    { endpoint = "/raw" }
         |  latest { endpoint = "/reports/latest" }
         |}
         |""".stripMargin
    Files.writeString(dir.resolve("strict.conf"), conf("strict"))
    Files.writeString(dir.resolve("variant.conf"), conf("variant"))
    spark = SparkSession.builder().master("local[1]").appName("ResponseContentTypeSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", dir.resolve("strict.conf").toAbsolutePath.toString)
      .config("spark.sql.catalog.apiv", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.apiv.config", dir.resolve("variant.conf").toAbsolutePath.toString)
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

  test("a JSON response with a charset gives the table its columns") {
    // Only an exact application/json counted: this table resolved with no columns.
    server.stubFor(get(urlPathEqualTo("/items")).willReturn(okJson("""[{"id": 1, "name": "a"}]""")))

    val df = spark.table("api.default.items")
    assertEquals(df.columns.toList.sorted, List("id", "name"))
    assertEquals(df.collect().map(_.getString(df.columns.indexOf("name"))).toList, List("a"))
  }

  test("in strict mode, a table whose endpoint has no JSON response fails, naming what's offered") {
    val ex = intercept[Exception](spark.table("api.default.report"))
    val msg = Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")
    assert(msg.contains("'report'") && msg.contains("text/csv") && msg.contains("variant"), msg)
  }

  test("in variant mode, the same table still loads") {
    assertEquals(spark.table("apiv.default.report").columns.toList, List("value"))
  }

  test("a configured path the spec describes as CSV doesn't borrow a same-named endpoint's columns") {
    // `listing` is /items' operationId. Matched by name, /report took /items' JSON columns.
    val ex = intercept[Exception](spark.table("api.default.listing"))
    val msg = Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")
    assert(msg.contains("'listing'") && msg.contains("text/csv"), msg)
  }

  test("a JSON response without a schema is reported as that, not as non-JSON") {
    val ex = intercept[Exception](spark.table("api.default.raw"))
    val msg = Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")
    assert(msg.contains("application/json") && msg.contains("no object schema"), msg)
  }

  test("a concrete CSV path doesn't take its columns from a JSON template that also matches it") {
    // /reports/latest is CSV; /reports/{id} is JSON and matches it as a template.
    val ex = intercept[Exception](spark.table("api.default.latest"))
    val msg = Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")
    assert(msg.contains("'latest'") && msg.contains("text/csv"), msg)
  }
}

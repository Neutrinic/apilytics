package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

/** COUNT pushdown planned and executed through a real SparkSession.
  *
  * The gap that let #212 ship: count config appeared only in tests that called readers
  * or config parsing directly. Nothing planned a query, and the failure was in planning
  * — `readSchema` advertised the table's columns while the reader returned one aggregate
  * value, so Spark's column-count assertion fired during optimization and no reader was
  * ever constructed. Only a query that goes through the optimizer catches that.
  */
class AggregationPushdownSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var configPath: Path = _

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()

    val spec = Path.of("src/test/resources/github-issues.yaml").toAbsolutePath.toString.replace("\\", "/")
    configPath = Files.createTempFile("apilytics-count", ".conf")
    Files.writeString(
      configPath,
      s"""openapi = "$spec"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = header, header-name = "Accept", header-value = "application/json" }
         |tables {
         |  issues {
         |    endpoint = "/repos/octocat/Hello-World/issues"
         |    count { param = "per_page", param-value = "1", response-path = "/total_count" }
         |  }
         |}
         |""".stripMargin
    )

    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("AggregationPushdownSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", configPath.toAbsolutePath.toString)
      .getOrCreate()
  }

  override def afterEach(context: AfterEach): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    if (server != null) server.stop()
    if (configPath != null) Files.deleteIfExists(configPath)
  }

  test("COUNT(*) pushdown plans and returns the API-reported count") {
    server.stubFor(
      get(urlPathEqualTo("/repos/octocat/Hello-World/issues"))
        .withQueryParam("per_page", equalTo("1"))
        .willReturn(okJson("""{"total_count": 42, "items": []}"""))
    )

    // Before the fix this threw during optimization:
    //   AssertionError: The data source returns unexpected number of columns
    val rows = spark.sql("SELECT COUNT(*) FROM api.default.issues").collect()

    assertEquals(rows.length, 1)
    assertEquals(rows.head.getLong(0), 42L)

    // Pushed down, not counted client-side: only the count request was made.
    server.verify(
      1,
      getRequestedFor(urlPathEqualTo("/repos/octocat/Hello-World/issues"))
        .withQueryParam("per_page", equalTo("1"))
    )
  }

  test("SUM is not pushed down but still returns the right answer") {
    // The API offers a SUM endpoint, but its result type is only known once the JSON
    // arrives, so pushing it would mean advertising a schema we cannot guarantee (#213).
    // Declining must degrade to a normal scan, not a crash: Spark reads the rows and
    // sums them itself.
    Files.writeString(
      configPath,
      Files.readString(configPath).replace(
        """count { param = "per_page", param-value = "1", response-path = "/total_count" }""",
        """aggregations { total { function = "sum", column = "number", endpoint = "/stats", response-path = "/total" } }"""
      )
    )
    spark.sql("CLEAR CACHE")

    server.stubFor(
      get(urlPathEqualTo("/repos/octocat/Hello-World/issues"))
        .willReturn(okJson("""[{"id": 1, "number": 10, "title": "a"}, {"id": 2, "number": 32, "title": "b"}]"""))
    )

    val rows = spark.sql("SELECT SUM(number) FROM api.default.issues").collect()

    assertEquals(rows.length, 1)
    assertEquals(rows.head.get(0).toString, "42", "Spark should have summed 10 + 32 itself")

    // The aggregate endpoint must not have been called — the push was declined.
    server.verify(0, getRequestedFor(urlPathEqualTo("/stats")))
  }

  test("COUNT(*) reports a whole number even when the API returns it as a decimal") {
    // The schema fixes COUNT to LongType, so a 42.0 response must still land as 42.
    server.stubFor(
      get(urlPathEqualTo("/repos/octocat/Hello-World/issues"))
        .withQueryParam("per_page", equalTo("1"))
        .willReturn(okJson("""{"total_count": 42.0, "items": []}"""))
    )

    val rows = spark.sql("SELECT COUNT(*) FROM api.default.issues").collect()
    assertEquals(rows.head.getLong(0), 42L)
  }
}

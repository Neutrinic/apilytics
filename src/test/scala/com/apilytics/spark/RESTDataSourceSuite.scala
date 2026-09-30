package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

import java.nio.file.{Files, Path}

/** `format("apilytics")` against a real SparkSession, compared with the catalog.
  *
  * The data source exists for platforms that own the catalog namespace (#257), so the
  * property that matters is that it is the *same* table: identical rows, the same pushdown,
  * the same streaming capability. Each test reads both ways where that comparison applies.
  */
class RESTDataSourceSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var configPath: Path = _

  private val issuesPath = "/repos/octocat/Hello-World/issues"

  private val issuesJson =
    """[
      |  {"id": 1, "number": 101, "title": "first",  "state": "open",   "created_at": "2026-01-01T00:00:00Z", "user": {"login": "a"}},
      |  {"id": 2, "number": 102, "title": "second", "state": "closed", "created_at": "2026-01-02T00:00:00Z", "user": {"login": "b"}},
      |  {"id": 3, "number": 103, "title": "third",  "state": "open",   "created_at": "2026-01-03T00:00:00Z", "user": {"login": "c"}}
      |]""".stripMargin

  private def writeConfig(extraTables: String = ""): Unit = {
    val spec = Path.of("src/test/resources/github-issues.yaml").toAbsolutePath.toString.replace("\\", "/")
    Files.writeString(
      configPath,
      s"""openapi = "$spec"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |tables {
         |  issues {
         |    endpoint = "$issuesPath"
         |    count { param = "per_page", param-value = "1", response-path = "/total_count" }
         |  }
         |  events {
         |    endpoint = "$issuesPath"
         |    checkpoint { mode = timestamp, timestamp-param = "since", timestamp-path = "/created_at" }
         |  }
         |  $extraTables
         |}
         |""".stripMargin
    )
  }

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    configPath = Files.createTempFile("apilytics-format", ".conf")
    writeConfig()

    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("RESTDataSourceSuite")
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

  private def viaFormat(table: String): DataFrame =
    spark.read.format("apilytics")
      .option("config", configPath.toAbsolutePath.toString)
      .option("table", table)
      .load()

  private def rows(df: DataFrame): Seq[String] =
    df.select("id", "title", "state").orderBy("id").collect().map(_.mkString("|")).toSeq

  test("format() returns the same rows and schema as the catalog") {
    server.stubFor(get(urlPathEqualTo(issuesPath)).willReturn(okJson(issuesJson)))

    val catalog = spark.table("api.default.issues")
    val format  = viaFormat("issues")

    assertEquals(format.schema, catalog.schema)
    assertEquals(rows(format), rows(catalog))
    assertEquals(rows(format), Seq("1|first|open", "2|second|closed", "3|third|open"))
  }

  test("pushdown is shared: COUNT(*) through format() calls only the count endpoint") {
    server.stubFor(
      get(urlPathEqualTo(issuesPath))
        .withQueryParam("per_page", equalTo("1"))
        .willReturn(okJson("""{"total_count": 42}"""))
    )

    assertEquals(viaFormat("issues").count(), 42L)

    // The count request, and no scan of the records.
    server.verify(1, getRequestedFor(urlPathEqualTo(issuesPath)).withQueryParam("per_page", equalTo("1")))
    server.verify(1, getRequestedFor(urlPathEqualTo(issuesPath)))
  }

  test("a SQL view over format() works where no catalog is available") {
    server.stubFor(get(urlPathEqualTo(issuesPath)).willReturn(okJson(issuesJson)))

    spark.sql(
      s"""CREATE TEMPORARY VIEW issues_view USING apilytics
         |OPTIONS (config '${configPath.toAbsolutePath.toString.replace("\\", "/")}', table 'issues')""".stripMargin
    )
    val titles = spark.sql("SELECT title FROM issues_view WHERE state = 'open' ORDER BY id")
      .collect().map(_.getString(0)).toSeq

    assertEquals(titles, Seq("first", "third"))
  }

  test("readStream.format() plans a V2 micro-batch source for a streamable table") {
    // Runs everywhere, unlike the end-to-end test below, because it writes no checkpoint.
    // It is the one that matters: implementing StreamSourceProvider makes load() build the
    // legacy relation eagerly, and a sourceSchema that threw made every table unstreamable
    // before any batch ran.
    val df = spark.readStream.format("apilytics")
      .option("config", configPath.toAbsolutePath.toString)
      .option("table", "events")
      .load()

    assert(df.isStreaming)
    val nodes = df.queryExecution.analyzed.collect { case p => p.getClass.getSimpleName }
    assert(nodes.contains("StreamingRelationV2"), s"expected the V2 streaming source, got: $nodes")
  }

  test("a streamable table can be read with readStream.format()") {
    // Spark's local checkpoint writer needs Hadoop's native helpers on Windows. CI and the
    // lab run this on Linux.
    assume(!System.getProperty("os.name").startsWith("Windows"), "needs Hadoop native helpers on Windows")
    // A fresh stream starts from now, so an AvailableNow run spans [now, now] and makes no
    // request. What this checks is the wiring: the table advertises micro-batch reads
    // through format() as it does through the catalog, and the query plans and completes.
    val q = spark.readStream.format("apilytics")
      .option("config", configPath.toAbsolutePath.toString)
      .option("table", "events")
      .load()
      .writeStream.format("memory").queryName("events_stream")
      .trigger(Trigger.AvailableNow())
      .start()
    q.awaitTermination(60000)

    assert(q.exception.isEmpty, q.exception.map(_.toString).getOrElse(""))
    assertEquals(spark.table("events_stream").count(), 0L)
  }

  test("a table that cannot stream is rejected by readStream.format(), naming what it needs") {
    val e = intercept[Exception] {
      spark.readStream.format("apilytics")
        .option("config", configPath.toAbsolutePath.toString)
        .option("table", "issues")
        .load()
        .writeStream.format("memory").queryName("issues_stream").start()
    }
    val msg = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")
    assert(msg.contains("Table 'issues' cannot be read as a stream"), msg)
    assert(msg.contains("mode = timestamp"), msg)
  }

  test("a missing 'config' or 'table' option fails naming the option") {
    val noConfig = intercept[IllegalArgumentException] {
      spark.read.format("apilytics").option("table", "issues").load()
    }
    assert(noConfig.getMessage.contains("'config'"), noConfig.getMessage)

    val noTable = intercept[IllegalArgumentException] {
      spark.read.format("apilytics").option("config", configPath.toAbsolutePath.toString).load()
    }
    assert(noTable.getMessage.contains("'table'"), noTable.getMessage)
  }

  test("an unknown table fails listing the tables the config defines") {
    val e = intercept[IllegalArgumentException](viaFormat("isues").schema)
    assert(e.getMessage.contains("isues"), e.getMessage)
    assert(e.getMessage.contains("events, issues"), e.getMessage)
  }

  test("an edited config takes effect on the next read") {
    // The loaded catalog is cached per config file. Keying on modification time as well as
    // path is what lets a config change be picked up without restarting the session.
    intercept[IllegalArgumentException](viaFormat("recent").schema)

    writeConfig(s"""recent { endpoint = "$issuesPath" }""")
    configPath.toFile.setLastModified(configPath.toFile.lastModified() + 5000)

    assertEquals(viaFormat("recent").schema, viaFormat("issues").schema)
  }
}

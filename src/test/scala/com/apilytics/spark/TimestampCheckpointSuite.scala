package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

/** A timestamp checkpoint and a filter on the same query parameter (#310), and the
  * timestamp the checkpoint saves (#311).
  */
class TimestampCheckpointSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var dir: Path = _

  private val spec =
    """openapi: 3.0.0
      |info: { title: issues, version: "1" }
      |paths:
      |  /issues:
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
      |                    items:
      |                      type: object
      |                      properties:
      |                        id: { type: integer }
      |                        updated_at: { type: string, format: date-time }
      |""".stripMargin

  private def checkpointFile: Path = dir.resolve("checkpoints").resolve("issues.checkpoint.json")

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    dir = Files.createTempDirectory("apilytics-timestamp")
    Files.writeString(dir.resolve("issues.yaml"), spec)
    val checkpoints = dir.resolve("checkpoints").toAbsolutePath.toString.replace("\\", "/")
    // The docs' own setup: `since` is both the filter on updated_at and the checkpoint's
    // timestamp-param.
    Files.writeString(dir.resolve("issues.conf"),
      s"""openapi = "issues.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |http { max-retries = 0, max-backoff = "0 seconds" }
         |pagination { style = cursor, cursor-path = "/next", cursor-param = "cursor" }
         |tables {
         |  issues {
         |    endpoint  = "/issues"
         |    data-path = "/items"
         |    filters = [ { param = "since", column = "updated_at", operators = ["gte"] } ]
         |    checkpoint {
         |      enabled = true, path = "$checkpoints", mode = timestamp
         |      timestamp-param = "since", timestamp-path = "/updated_at"
         |    }
         |  }
         |}
         |""".stripMargin)
    spark = SparkSession.builder().master("local[1]").appName("TimestampCheckpointSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", dir.resolve("issues.conf").toAbsolutePath.toString)
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

  private def saveCheckpoint(ts: String): Unit = {
    Files.createDirectories(checkpointFile.getParent)
    Files.writeString(checkpointFile, s"""{"type": "timestamp", "value": "$ts"}""")
  }

  private def sentSince: List[String] =
    server.getAllServeEvents.asScala.toList
      .flatMap(e => Option(e.getRequest.queryParameter("since")).filter(_.isPresent).map(_.firstValue()))

  test("a WHERE on the checkpoint's parameter isn't replaced by an older checkpoint (#310)") {
    // The API answers `since=2020-...` with everything updated since then.
    server.stubFor(get(urlPathEqualTo("/issues"))
      .willReturn(okJson(
        """{"items": [{"id": 1, "updated_at": "2021-06-01T00:00:00Z"},
          |           {"id": 2, "updated_at": "2025-03-01T00:00:00Z"}], "next": ""}""".stripMargin)))
    saveCheckpoint("2020-01-01T00:00:00Z")

    // Pushed as since=2025-01-01, then replaced by the checkpoint's 2020 value: Spark had
    // dropped the predicate, so the 2021 issue came back.
    val ids = spark.sql(
      "SELECT id FROM api.default.issues WHERE updated_at >= TIMESTAMP '2025-01-01 00:00:00'"
    ).collect().map(_.getAs[Number](0).longValue).toList

    assertEquals(ids, List(2L))
    assertEquals(sentSince, List("2020-01-01T00:00:00Z"), "the checkpoint's value is what's sent")
  }

  test("the scan builder keeps a predicate on the checkpoint's parameter for Spark (#310)") {
    // Batch and streaming scans come from the same builder.
    val table = spark.sessionState.catalogManager.catalog("api").asInstanceOf[TableCatalog]
      .loadTable(Identifier.of(Array("default"), "issues"))
    val builder = table.asInstanceOf[RESTTable].newScanBuilder(CaseInsensitiveStringMap.empty())
      .asInstanceOf[RESTScanBuilder]
    val predicate = new Predicate(">=", Array[Expression](
      Expressions.column("updated_at"),
      Expressions.literal(UTF8String.fromString("2025-01-01T00:00:00Z"))
    ))

    assertEquals(builder.pushPredicates(Array(predicate)).toList, List(predicate))
    assertEquals(builder.pushedPredicates().toList, Nil)
    assertEquals(builder.pushedParamsForTest, Map.empty[String, String])
  }

  test("the checkpoint saves the latest timestamp of the read, not of its last page (#311)") {
    // Newest first, as GitHub serves by default. The last page's maximum is the oldest.
    server.stubFor(get(urlPathEqualTo("/issues")).withQueryParam("cursor", absent())
      .willReturn(okJson("""{"items": [{"id": 3, "updated_at": "2025-03-01T00:00:00Z"}], "next": "c2"}""")))
    server.stubFor(get(urlPathEqualTo("/issues")).withQueryParam("cursor", equalTo("c2"))
      .willReturn(okJson("""{"items": [{"id": 2, "updated_at": "2025-02-01T00:00:00Z"}], "next": "c3"}""")))
    server.stubFor(get(urlPathEqualTo("/issues")).withQueryParam("cursor", equalTo("c3"))
      .willReturn(okJson("""{"items": [{"id": 1, "updated_at": "2025-01-01T00:00:00Z"}], "next": ""}""")))

    assertEquals(spark.sql("SELECT id FROM api.default.issues").count(), 3L)
    assert(Files.readString(checkpointFile).contains("2025-03-01T00:00:00Z"), Files.readString(checkpointFile))
  }

  test("an empty last page doesn't replace the saved timestamp with a cursor (#311)") {
    server.stubFor(get(urlPathEqualTo("/issues")).withQueryParam("cursor", absent())
      .willReturn(okJson("""{"items": [{"id": 1, "updated_at": "2025-03-01T00:00:00Z"}], "next": "c2"}""")))
    server.stubFor(get(urlPathEqualTo("/issues")).withQueryParam("cursor", equalTo("c2"))
      .willReturn(okJson("""{"items": [], "next": ""}""")))

    assertEquals(spark.sql("SELECT id FROM api.default.issues").count(), 1L)
    val saved = Files.readString(checkpointFile)
    assert(saved.contains("\"timestamp\"") && saved.contains("2025-03-01T00:00:00Z"), saved)
  }

  test("timestamps compare as instants, not strings (#311)") {
    import RESTColumnarPartitionReader.laterTimestamp
    // As strings, ".5Z" sorts before "Z", and offsets don't order at all.
    assertEquals(laterTimestamp("2026-01-15T10:00:00Z", "2026-01-15T10:00:00.5Z"), "2026-01-15T10:00:00.5Z")
    assertEquals(laterTimestamp("2026-01-15T11:00:00+02:00", "2026-01-15T10:00:00Z"), "2026-01-15T10:00:00Z")
    // Unparseable: fall back to the string order.
    assertEquals(laterTimestamp("b", "a"), "b")
  }
}

package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.{Row, SparkSession}

import java.nio.file.{Files, Path}

/** A batch checkpoint advances only when the read's task succeeds (#280).
  *
  * The state used to be saved in the page stream's finaliser, which also runs when the read
  * fails or is cancelled. A failed run then moved the checkpoint past records it never
  * delivered, and the next run skipped them.
  */
class CheckpointSparkSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var dir: Path = _

  private val spec =
    """openapi: 3.0.0
      |info: { title: events, version: "1" }
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
      |""".stripMargin

  private def checkpointFile: Path = dir.resolve("checkpoints").resolve("events.checkpoint.json")

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    // Two pages: the first names the cursor of the second, which is the last.
    server.stubFor(get(urlPathEqualTo("/events")).withQueryParam("cursor", absent())
      .willReturn(okJson("""{"items": [{"id": 1}], "next": "c2"}""")))
    server.stubFor(get(urlPathEqualTo("/events")).withQueryParam("cursor", equalTo("c2"))
      .willReturn(okJson("""{"items": [{"id": 2}], "next": ""}""")))

    dir = Files.createTempDirectory("apilytics-checkpoint")
    Files.writeString(dir.resolve("events.yaml"), spec)
    val checkpoints = dir.resolve("checkpoints").toAbsolutePath.toString.replace("\\", "/")
    Files.writeString(
      dir.resolve("events.conf"),
      s"""openapi = "events.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |http { max-retries = 0, max-backoff = "0 seconds" }
         |pagination { style = cursor, cursor-path = "/next", cursor-param = "cursor" }
         |tables {
         |  events {
         |    endpoint  = "/events"
         |    data-path = "/items"
         |    checkpoint { enabled = true, path = "$checkpoints", mode = cursor }
         |  }
         |}
         |""".stripMargin
    )

    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("CheckpointSparkSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", dir.resolve("events.conf").toAbsolutePath.toString)
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

  test("a read that succeeds saves the checkpoint") {
    val ids = spark.sql("SELECT id FROM api.default.events").collect().map(_.getAs[Number](0).longValue).sorted.toList

    assertEquals(ids, List(1L, 2L))
    assert(Files.exists(checkpointFile), "no checkpoint was saved after a successful read")
    // The last page has no next cursor, so the resume point is the cursor that fetched it.
    assert(Files.readString(checkpointFile).contains("\"c2\""), Files.readString(checkpointFile))
  }

  test("a read that fails partway saves nothing") {
    server.stubFor(get(urlPathEqualTo("/events")).withQueryParam("cursor", equalTo("c2"))
      .willReturn(serverError()))

    intercept[Exception](spark.sql("SELECT id FROM api.default.events").collect())

    assert(!Files.exists(checkpointFile),
      s"a failed read saved a checkpoint: ${Files.readString(checkpointFile)}")
  }

  test("a read whose task fails downstream saves nothing") {
    // Every page is read, but the task fails after the rows reach it, as a failed write would.
    intercept[Exception](
      spark.table("api.default.events").foreach((_: Row) => throw new RuntimeException("sink failed"))
    )

    assert(!Files.exists(checkpointFile),
      s"a failed task saved a checkpoint: ${Files.readString(checkpointFile)}")
  }
}

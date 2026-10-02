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
    // The task reads every row first, so the read itself completes, then fails as a failed
    // write would. Only the task-failure gate can stop the save here: throwing on the first
    // row would cancel the read before it finished, and pass for the wrong reason.
    val error = intercept[Exception](
      spark.table("api.default.events").foreachPartition { (rows: Iterator[Row]) =>
        val read = rows.size
        throw new RuntimeException(s"sink failed after reading $read rows")
      }
    )

    assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
      .exists(e => String.valueOf(e.getMessage).contains("sink failed after reading 2 rows")),
      s"the task failed for another reason: $error")
    assert(!Files.exists(checkpointFile),
      s"a failed task saved a checkpoint: ${Files.readString(checkpointFile)}")
  }

  test("a task killed after its read finished saves nothing") {
    // The window CodeRabbit pointed at: the read completes, the task is killed, and Spark
    // reports the kill only after the completion callbacks have run. The task reads every
    // row, cancels its own job, waits until Spark has marked it interrupted, then returns
    // normally, so nothing fails it and only the interruption check can stop the save.
    // Local mode runs the task in the driver's JVM, which is what lets it reach the context.
    // Cancelling the job fails the driver's call at once, while the killed task is still
    // running; its completion listener runs later. Wait for the task to end before looking.
    val taskEnded = new java.util.concurrent.CountDownLatch(1)
    spark.sparkContext.addSparkListener(new org.apache.spark.scheduler.SparkListener {
      override def onTaskEnd(end: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit = taskEnded.countDown()
    })
    spark.sparkContext.setJobGroup("checkpoint-kill", "kill after read", interruptOnCancel = false)
    try {
      intercept[Exception](
        spark.table("api.default.events").foreachPartition { (rows: Iterator[Row]) =>
          rows.size
          org.apache.spark.SparkContext.getOrCreate().cancelJobGroup("checkpoint-kill")
          val ctx = org.apache.spark.TaskContext.get()
          val deadline = System.nanoTime() + 10L * 1000 * 1000 * 1000
          while (!ctx.isInterrupted() && System.nanoTime() < deadline) Thread.sleep(20)
        }
      )
    } finally spark.sparkContext.clearJobGroup()

    assert(taskEnded.await(30, java.util.concurrent.TimeUnit.SECONDS), "the killed task never ended")
    assert(!Files.exists(checkpointFile),
      s"a killed task saved a checkpoint: ${Files.readString(checkpointFile)}")
  }
}

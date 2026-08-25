package com.apilytics.spark

import com.apilytics.core.checkpoint.CheckpointState
import com.apilytics.core.config._
import com.apilytics.core.openapi.Endpoint
import com.apilytics.core.rest.RestHandle
import com.apilytics.core.schema.{SchemaMapper, SourceSchema}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.connector.catalog.TableCapability

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Micro-batch streaming over a REST endpoint (#36).
  *
  * A pull-based API cannot say how much is available without being asked, so the stream
  * uses the clock as its offset and relies on a "changed since" parameter. These tests
  * pin the consequences of that: which tables may stream at all, that each batch asks for
  * the right window, and that consecutive batches do not overlap.
  */
class MicroBatchStreamSuite extends FunSuite {

  private var server: WireMockServer = _

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
  }

  override def afterEach(context: AfterEach): Unit = server.stop()

  private val recordSchema = SourceSchema.ObjectType(
    Map("id" -> SourceSchema.IntegerType(), "updated_at" -> SourceSchema.StringType())
  )

  private def checkpoint(
      mode: CheckpointMode = CheckpointMode.Timestamp,
      param: Option[String] = Some("since"),
      path: Option[String] = Some("/updated_at")
  ) = CheckpointConfig(
    enabled = false,
    path = "",
    mode = mode,
    timestampPath = path,
    timestampParam = param
  )

  private def table(cc: Option[CheckpointConfig]): RESTTable = {
    val sourceConfig = SourceConfig(
      openapi = "test.yaml",
      auth = AuthConfig(authType = AuthType.None),
      pagination = PaginationConfig(style = PaginationStyle.None),
      schema = SchemaConfig(),
      http = HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 10.seconds)
    )
    val tc = TableConfig(endpoint = "/issues", checkpoint = cc)

    new RESTTable(
      tableName = "issues",
      arrowSchema = SchemaMapper.toArrowSchema(recordSchema),
      handle = RestHandle("/issues", s"http://localhost:${server.port()}", Some(tc)),
      tableConfig = Some(tc),
      sourceConfig = sourceConfig,
      baseUrl = s"http://localhost:${server.port()}"
    )
  }

  private def scanFor(t: RESTTable): RESTScan =
    new RESTScan(t, SchemaMapper.toArrowSchema(recordSchema), None, Map.empty, None)

  // --- Capability ---

  test("a timestamp-checkpointed table advertises MICRO_BATCH_READ") {
    assert(table(Some(checkpoint())).capabilities().contains(TableCapability.MICRO_BATCH_READ))
  }

  test("cursor and offset checkpointing do not advertise streaming") {
    // Neither can answer "how much is available" without fetching, so claiming the
    // capability would defer the failure from analysis to run time.
    for (mode <- List(CheckpointMode.Cursor, CheckpointMode.Offset)) {
      val caps = table(Some(checkpoint(mode = mode))).capabilities()
      assert(!caps.contains(TableCapability.MICRO_BATCH_READ), s"$mode advertised streaming")
      assert(caps.contains(TableCapability.BATCH_READ), s"$mode lost batch read")
    }
  }

  test("timestamp mode without a query parameter does not advertise streaming") {
    // Without the parameter there is no way to ask for a window, so every batch would
    // re-read the endpoint in full.
    val caps = table(Some(checkpoint(param = None))).capabilities()
    assert(!caps.contains(TableCapability.MICRO_BATCH_READ))
  }

  test("a table with no checkpoint config does not advertise streaming") {
    assert(!table(None).capabilities().contains(TableCapability.MICRO_BATCH_READ))
  }

  test("asking a non-streaming table for a stream fails with a usable message") {
    val e = intercept[IllegalStateException] {
      scanFor(table(None)).toMicroBatchStream("/tmp/cp")
    }
    assert(e.getMessage.contains("timestamp-param"), e.getMessage)
  }

  // --- Offsets ---

  test("offsets round-trip through their JSON form") {
    val stream = scanFor(table(Some(checkpoint()))).toMicroBatchStream("/tmp/cp")
    val offset = TimestampOffset("2026-01-15T10:30:00Z")

    assertEquals(stream.deserializeOffset(offset.json()), offset)
  }

  test("offsets share the batch checkpoint's on-disk shape") {
    // Same representation, so a stream offset and a batch checkpoint stay readable by
    // the same code rather than drifting into two formats.
    val json = TimestampOffset("2026-01-15T10:30:00Z").json()
    assertEquals(
      CheckpointState.fromJsonString(json),
      Right(CheckpointState.TimestampValue("2026-01-15T10:30:00Z"))
    )
  }

  test("an offset written by another checkpoint mode is rejected, not misread") {
    val cursorJson = CheckpointState.CursorValue("abc").toJson.noSpaces
    val e = intercept[IllegalArgumentException] { TimestampOffset.fromJson(cursorJson) }
    assert(e.getMessage.contains("timestamp offset"), e.getMessage)
  }

  test("latestOffset advances with the clock") {
    val stream = scanFor(table(Some(checkpoint()))).toMicroBatchStream("/tmp/cp")
    val first  = stream.latestOffset().asInstanceOf[TimestampOffset]
    Thread.sleep(1100)
    val second = stream.latestOffset().asInstanceOf[TimestampOffset]
    assert(second.value > first.value, s"${second.value} did not advance past ${first.value}")
  }

  // --- Planning ---

  test("each batch asks the API for records changed since its start offset") {
    val stream = scanFor(table(Some(checkpoint()))).toMicroBatchStream("/tmp/cp")
    val parts = stream.planInputPartitions(
      TimestampOffset("2026-01-15T10:00:00Z"),
      TimestampOffset("2026-01-15T10:05:00Z")
    )

    assertEquals(parts.length, 1)
    val p = parts.head.asInstanceOf[RESTInputPartition]
    assertEquals(p.pushedParams.get("since"), Some("2026-01-15T10:00:00Z"))
    assertEquals(p.streamBound.map(_.endExclusive), Some("2026-01-15T10:05:00Z"))
  }

  test("an empty interval makes no request at all") {
    val stream = scanFor(table(Some(checkpoint()))).toMicroBatchStream("/tmp/cp")
    val same = TimestampOffset("2026-01-15T10:00:00Z")
    assertEquals(stream.planInputPartitions(same, same).length, 0)
  }

  test("streaming partitions carry no batch checkpoint config") {
    // Spark owns the offset log for a stream. Leaving checkpointConfig set would have
    // the reader also load and overwrite the batch checkpoint file underneath it.
    val stream = scanFor(table(Some(checkpoint()))).toMicroBatchStream("/tmp/cp")
    val p = stream
      .planInputPartitions(TimestampOffset("2026-01-15T10:00:00Z"), TimestampOffset("2026-01-15T10:05:00Z"))
      .head
      .asInstanceOf[RESTInputPartition]

    assertEquals(p.checkpointConfig, None)
  }

  // --- Reading ---

  test("records at or after the batch end are trimmed so batches do not overlap") {
    // The API filter is one-sided: asking for "since 10:00" also returns 10:07, which
    // belongs to the next batch. Without trimming it would be delivered twice.
    server.stubFor(
      get(urlPathEqualTo("/issues"))
        .withQueryParam("since", equalTo("2026-01-15T10:00:00Z"))
        .willReturn(okJson(
          """[{"id":1,"updated_at":"2026-01-15T10:01:00Z"},
            | {"id":2,"updated_at":"2026-01-15T10:04:00Z"},
            | {"id":3,"updated_at":"2026-01-15T10:07:00Z"}]""".stripMargin
        ))
    )

    val stream = scanFor(table(Some(checkpoint()))).toMicroBatchStream("/tmp/cp")
    val part = stream
      .planInputPartitions(TimestampOffset("2026-01-15T10:00:00Z"), TimestampOffset("2026-01-15T10:05:00Z"))
      .head
      .asInstanceOf[RESTInputPartition]

    val reader = new RESTColumnarPartitionReader(part)
    val ids = try {
      var acc = List.empty[Int]
      while (reader.next()) {
        val b = reader.get()
        acc ++= (0 until b.numRows()).map(i => b.column(0).getInt(i))
      }
      acc
    } finally reader.close()

    assertEquals(ids, List(1, 2), "record at 10:07 belongs to the next batch")
  }

  test("a record with no timestamp is kept rather than dropped") {
    // It cannot be shown to belong to a later batch, and dropping loses data outright
    // where keeping it can only duplicate.
    server.stubFor(
      get(urlPathEqualTo("/issues"))
        .withQueryParam("since", equalTo("2026-01-15T10:00:00Z"))
        .willReturn(okJson("""[{"id":1},{"id":2,"updated_at":"2026-01-15T10:09:00Z"}]"""))
    )

    val stream = scanFor(table(Some(checkpoint()))).toMicroBatchStream("/tmp/cp")
    val part = stream
      .planInputPartitions(TimestampOffset("2026-01-15T10:00:00Z"), TimestampOffset("2026-01-15T10:05:00Z"))
      .head
      .asInstanceOf[RESTInputPartition]

    val reader = new RESTColumnarPartitionReader(part)
    val ids = try {
      var acc = List.empty[Int]
      while (reader.next()) {
        val b = reader.get()
        acc ++= (0 until b.numRows()).map(i => b.column(0).getInt(i))
      }
      acc
    } finally reader.close()

    assertEquals(ids, List(1))
  }
}

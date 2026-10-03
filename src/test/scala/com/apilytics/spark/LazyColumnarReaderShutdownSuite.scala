package com.apilytics.spark

import com.apilytics.core.config._
import com.apilytics.core.rest.RestHandle
import com.apilytics.core.openapi.Endpoint
import com.apilytics.core.schema.{SchemaMapper, SourceSchema}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite

import scala.concurrent.duration._

/** Shutdown behaviour of the lazy columnar reader.
  *
  * Spark routinely stops reading before a partition is exhausted — a satisfied LIMIT,
  * a failed task, a cancelled job — and then calls `close()`. At that moment the
  * producer fiber is typically parked on a full bounded queue, which is the case this
  * suite pins down.
  */
class LazyColumnarReaderShutdownSuite extends FunSuite {

  private var server: WireMockServer = _

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
  }

  override def afterEach(context: AfterEach): Unit = server.stop()

  private val recordSchema = SourceSchema.ObjectType(
    Map("id" -> SourceSchema.IntegerType(), "payload" -> SourceSchema.StringType())
  )

  private val endpoint = Endpoint(
    path = "/items",
    operationId = Some("listItems"),
    responseSchema = recordSchema,
    queryParams = Nil
  )

  private def stubPages(pages: Int, perPage: Int): Unit = {
    val filler = "x" * 256
    (0 until pages).foreach { page =>
      val offset = page * perPage
      val records = (0 until perPage).map(i => s"""{"id": ${offset + i}, "payload": "$filler"}""")
      server.stubFor(
        get(urlPathEqualTo("/items"))
          .withQueryParam("offset", equalTo(offset.toString))
          .willReturn(okJson(s"""{"results": [${records.mkString(",")}]}"""))
      )
    }
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("offset", equalTo((pages * perPage).toString))
        .willReturn(okJson("""{"results": []}"""))
    )
  }

  private def partition(perPage: Int, batchSize: Int, prefetch: Int): RESTInputPartition = {
    val sourceConfig = SourceConfig(
      openapi = "test.yaml",
      auth = AuthConfig(
        authType = AuthType.Header,
        headerName = Some("Accept"),
        headerValue = Some("application/json")
      ),
      pagination = PaginationConfig(
        style = PaginationStyle.Offset,
        offsetParam = Some("offset"),
        pageSizeParam = Some("limit"),
        maxPageSize = perPage,
        resultsPath = Some("/results")
      ),
      schema = SchemaConfig(arrowBatchSize = batchSize, prefetchBatches = prefetch),
      http = HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 10.seconds)
    )

    RESTInputPartition(
      handle = RestHandle(
        endpoint.path,
        s"http://localhost:${server.port()}",
        Some(TableConfig(endpoint = "/items", dataPath = Some("/results")))
      ),
      tableConfig = Some(TableConfig(endpoint = "/items", dataPath = Some("/results"))),
      sourceConfig = sourceConfig,
      baseUrl = s"http://localhost:${server.port()}",
      arrowSchemaJson = SchemaMapper.toArrowSchema(recordSchema).toJson,
      pushedParams = Map.empty,
      pushedLimit = None
    )
  }

  /** Run `body` on a daemon thread, returning false if it has not finished in time.
    *
    * An exception `body` throws is rethrown here. It used to be lost with the thread, so a
    * close() that failed — leaking Arrow memory, say — passed as long as it returned (#307).
    */
  private def completesWithin(timeout: FiniteDuration)(body: => Unit): Boolean = {
    @volatile var failure: Throwable = null
    val t = new Thread(() => try body catch { case e: Throwable => failure = e })
    t.setDaemon(true)
    t.start()
    t.join(timeout.toMillis)
    if (failure != null) throw failure
    !t.isAlive
  }

  test("close() returns when the producer is parked on a full queue") {
    stubPages(pages = 20, perPage = 50)
    val reader = new RESTColumnarPartitionReader(partition(perPage = 50, batchSize = 25, prefetch = 1))

    // Consume nothing, so the queue fills and the producer blocks offering the next
    // batch. The producer's end-of-stream finalizer is uncancelable, so if close()
    // cancels without draining, that finalizer parks forever offering the sentinel
    // into a full queue and cancellation never completes.
    Thread.sleep(2000)

    assert(
      completesWithin(15.seconds)(reader.close()),
      "close() deadlocked: producer's uncancelable finalizer blocked offering the sentinel into a full queue"
    )
  }

  test("close() returns after a partial read") {
    stubPages(pages = 20, perPage = 50)
    val reader = new RESTColumnarPartitionReader(partition(perPage = 50, batchSize = 25, prefetch = 2))

    // The LIMIT-satisfied shape: take a couple of batches, then stop early.
    assert(reader.next())
    assert(reader.next())

    assert(
      completesWithin(15.seconds)(reader.close()),
      "close() deadlocked after a partial read"
    )
  }

  test("close() after one batch releases every batch it allocated (#307)") {
    // Spark stops a LIMIT, show() or take() after the rows it needs, usually while the
    // producer holds a converted batch it has not yet offered to the full queue. close()
    // cancelled the producer and that batch was never closed, so the allocator found the
    // leak and threw "Memory was leaked by query", failing about 40% of these.
    stubPages(pages = 20, perPage = 50)
    (1 to 50).foreach { i =>
      val reader = new RESTColumnarPartitionReader(partition(perPage = 50, batchSize = 25, prefetch = 1))
      assert(reader.next())
      try reader.close()
      catch { case e: Throwable => fail(s"close() failed on iteration $i: $e", e) }
    }
  }

  test("close() returns after the stream is fully drained") {
    stubPages(pages = 2, perPage = 50)
    val reader = new RESTColumnarPartitionReader(partition(perPage = 50, batchSize = 25, prefetch = 2))

    while (reader.next()) ()

    assert(
      completesWithin(15.seconds)(reader.close()),
      "close() deadlocked after a full read"
    )
  }
}

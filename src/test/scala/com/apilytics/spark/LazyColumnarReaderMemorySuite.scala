package com.apilytics.spark

import com.apilytics.core.config._
import com.apilytics.core.openapi.{Endpoint, OpenAPISchema}
import com.apilytics.core.schema.SchemaMapper
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite

import scala.concurrent.duration._

/** Memory characteristics of the lazy columnar reader.
  *
  * `LazyColumnarReader` streams Arrow batches through a bounded queue, so peak
  * memory is meant to track `prefetch-batches * arrow-batch-size` rather than the
  * total size of the partition. These tests pin that contract down by measuring
  * the Arrow allocator's high-water mark while reading partitions of very
  * different sizes.
  */
class LazyColumnarReaderMemorySuite extends FunSuite {

  private var server: WireMockServer = _

  // Response caching is off by default, so repeated reads in a test really re-fetch.
  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
  }

  override def afterEach(context: AfterEach): Unit = server.stop()

  private val recordSchema = OpenAPISchema.ObjectType(
    Map(
      "id"      -> OpenAPISchema.IntegerType(),
      "name"    -> OpenAPISchema.StringType(),
      "payload" -> OpenAPISchema.StringType()
    )
  )

  private val endpoint = Endpoint(
    path = "/items",
    operationId = Some("listItems"),
    responseSchema = recordSchema,
    queryParams = Nil
  )

  /** Serve `pages` pages of `perPage` records via offset pagination, then an empty page. */
  private def stubPages(pages: Int, perPage: Int): Unit = {
    // Each record carries a chunky payload so Arrow allocation is easy to see.
    val filler = "x" * 512

    (0 until pages).foreach { page =>
      val offset = page * perPage
      val records = (0 until perPage).map { i =>
        s"""{"id": ${offset + i}, "name": "item-${offset + i}", "payload": "$filler"}"""
      }
      server.stubFor(
        get(urlPathEqualTo("/items"))
          .withQueryParam("offset", equalTo(offset.toString))
          .willReturn(okJson(s"""{"results": [${records.mkString(",")}]}"""))
      )
    }

    // Terminal empty page stops offset pagination.
    server.stubFor(
      get(urlPathEqualTo("/items"))
        .withQueryParam("offset", equalTo((pages * perPage).toString))
        .willReturn(okJson("""{"results": []}"""))
    )
  }

  private def partitionFor(batchSize: Int, prefetch: Int, perPage: Int): RESTInputPartition = {
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
      endpoint = endpoint,
      tableConfig = Some(TableConfig(endpoint = "/items", dataPath = Some("/results"))),
      sourceConfig = sourceConfig,
      baseUrl = s"http://localhost:${server.port()}",
      arrowSchemaJson = SchemaMapper.toArrowSchema(recordSchema).toJson,
      pushedParams = Map.empty,
      pushedLimit = None
    )
  }

  /** Read the partition to exhaustion, returning (rows read, peak Arrow bytes).
    *
    * `perBatchDelay` slows the consumer so the producer runs ahead and actually
    * fills the queue — prefetch is an upper bound, so it only shows up in the
    * high-water mark when Spark is the slower side.
    */
  private def readAll(partition: RESTInputPartition, perBatchDelay: Duration = Duration.Zero): (Int, Long) = {
    val reader = new RESTColumnarPartitionReader(partition)
    try {
      var rows = 0
      while (reader.next()) {
        rows += reader.get().numRows()
        if (perBatchDelay > Duration.Zero) Thread.sleep(perBatchDelay.toMillis)
      }
      (rows, reader.peakAllocatedBytes)
    } finally reader.close()
  }

  test("peak memory tracks batch size, not total records") {
    val batchSize = 256
    val perPage = 256

    stubPages(pages = 1, perPage = perPage)
    val (smallRows, smallPeak) = readAll(partitionFor(batchSize, prefetch = 2, perPage))

    server.resetAll()
    stubPages(pages = 20, perPage = perPage)
    val (largeRows, largePeak) = readAll(partitionFor(batchSize, prefetch = 2, perPage))

    assertEquals(smallRows, perPage)
    assertEquals(largeRows, perPage * 20, "must actually read every page")

    // 20x the data. If batches were materialised eagerly this would grow ~20x;
    // with the bounded queue only prefetch+in-flight batches are ever live.
    assert(
      largePeak <= smallPeak * 2,
      s"peak grew with partition size: $smallPeak bytes for $smallRows rows vs $largePeak bytes for $largeRows rows"
    )
  }

  test("peak memory scales with arrow-batch-size") {
    val perPage = 512
    stubPages(pages = 8, perPage = perPage)

    val (smallRows, smallPeak) = readAll(partitionFor(batchSize = 64, prefetch = 2, perPage))
    val (largeRows, largePeak) = readAll(partitionFor(batchSize = 512, prefetch = 2, perPage))

    assertEquals(smallRows, largeRows, "both runs must read the same data")

    // Guards against the bound above being vacuous: if peak were independent of
    // batch size, it would not be tracking live batches at all.
    assert(
      largePeak > smallPeak,
      s"arrow-batch-size had no effect on peak memory: 64 -> $smallPeak bytes, 512 -> $largePeak bytes"
    )
  }

  test("prefetch-batches raises the in-flight ceiling when the consumer lags") {
    val batchSize = 256
    val perPage = 256
    stubPages(pages = 12, perPage = perPage)

    // Prefetch is an upper bound, so it only becomes observable when Spark consumes
    // slower than the API delivers. Without the delay both runs sit at one live batch.
    val delay = 25.millis
    val (_, tightPeak) = readAll(partitionFor(batchSize, prefetch = 1, perPage), delay)
    val (_, loosePeak) = readAll(partitionFor(batchSize, prefetch = 8, perPage), delay)

    assert(
      loosePeak > tightPeak,
      s"prefetch-batches had no effect on peak memory: prefetch=1 -> $tightPeak bytes, prefetch=8 -> $loosePeak bytes"
    )
  }
}

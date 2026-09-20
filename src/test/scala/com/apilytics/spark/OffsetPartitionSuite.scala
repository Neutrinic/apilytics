package com.apilytics.spark

import com.apilytics.core.config._
import com.apilytics.core.rest.RestHandle
import com.apilytics.core.schema.{SchemaMapper, SourceSchema}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite

import scala.concurrent.duration._

/** Offset partitioning (#248).
  *
  * Splitting an offset-paginated endpoint needs both halves: each partition starts the
  * paginator at its own offset *and* stops it after one window. A start on its own leaves
  * every partition running to the end of the endpoint, which returns the whole dataset
  * once per partition — measured at 5404 rows for 1351 distinct records against PokeAPI.
  */
class OffsetPartitionSuite extends FunSuite {

  private var server: WireMockServer = _

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
  }

  override def afterEach(context: AfterEach): Unit = server.stop()

  private val recordSchema = SourceSchema.ObjectType(Map("id" -> SourceSchema.IntegerType()))

  /** Serves `total` records, honouring offset and limit like a normal paginated endpoint.
    *
    * Any offset beyond the data returns an empty page, which is what a real endpoint
    * does and what terminates pagination. A 404 there would be the stub misbehaving,
    * not the API.
    */
  private def stubRecords(total: Int, pageSize: Int): Unit = {
    server.stubFor(
      get(urlPathEqualTo("/records")).atPriority(10)
        .willReturn(okJson("{\"results\":[]}"))
    )

    (0 until total by pageSize).foreach { off =>
      val ids = (off until math.min(off + pageSize, total)).map(i => s"""{"id":$i}""")
      server.stubFor(
        get(urlPathEqualTo("/records"))
          .withQueryParam("offset", equalTo(off.toString)).atPriority(1)
          .willReturn(okJson(s"""{"results":[${ids.mkString(",")}]}"""))
      )
    }
  }

  private def tableWith(partition: Option[PartitionConfig], pageSize: Int): RESTTable = {
    val sourceConfig = SourceConfig(
      openapi = "test.yaml",
      auth = AuthConfig(authType = AuthType.None),
      pagination = PaginationConfig(
        style = PaginationStyle.Offset,
        offsetParam = Some("offset"),
        pageSizeParam = Some("limit"),
        maxPageSize = pageSize,
        resultsPath = Some("/results")
      ),
      schema = SchemaConfig(),
      http = HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 10.seconds)
    )
    val tc = TableConfig(endpoint = "/records", dataPath = Some("/results"), partition = partition)

    new RESTTable(
      tableName = "records",
      arrowSchema = SchemaMapper.toArrowSchema(recordSchema),
      handle = RestHandle("/records", s"http://localhost:${server.port()}", Some(tc)),
      tableConfig = Some(tc),
      sourceConfig = sourceConfig,
      baseUrl = s"http://localhost:${server.port()}"
    )
  }

  private def idsPerPartition(t: RESTTable): List[List[Int]] = {
    val scan = new RESTScan(t, SchemaMapper.toArrowSchema(recordSchema), None, Map.empty, None)
    scan.planInputPartitions().toList.map { p =>
      val reader = new RESTColumnarPartitionReader(p.asInstanceOf[RESTInputPartition])
      try {
        var acc = List.empty[Int]
        while (reader.next()) {
          val b = reader.get()
          acc ++= (0 until b.numRows()).map(i => b.column(0).getInt(i))
        }
        acc
      } finally reader.close()
    }
  }

  test("each partition reads only its own window") {
    stubRecords(total = 400, pageSize = 50)
    val perPartition =
      idsPerPartition(tableWith(Some(PartitionConfig.Offset(size = 100, count = 4)), 50))

    assertEquals(perPartition.length, 4)
    assertEquals(perPartition.map(_.length), List(100, 100, 100, 100), "windows are not equal")
    assertEquals(perPartition.head, (0 until 100).toList, "first window is not [0, 100)")
    assertEquals(perPartition(2), (200 until 300).toList, "third window is not [200, 300)")
  }

  test("partitions are disjoint and cover the endpoint exactly once") {
    // The failure this replaces returned every record in every partition.
    stubRecords(total = 400, pageSize = 50)
    val all = idsPerPartition(tableWith(Some(PartitionConfig.Offset(size = 100, count = 4)), 50)).flatten

    assertEquals(all.length, 400, "total row count")
    assertEquals(all.distinct.length, 400, "records were returned more than once")
    assertEquals(all.sorted, (0 until 400).toList, "coverage is not exact")
  }

  test("a window past the end of the endpoint yields nothing") {
    stubRecords(total = 150, pageSize = 50)
    val perPartition =
      idsPerPartition(tableWith(Some(PartitionConfig.Offset(size = 100, count = 4)), 50))

    assertEquals(perPartition.map(_.length), List(100, 50, 0, 0))
    assertEquals(perPartition.flatten.distinct.length, 150)
  }

  test("without partitioning the endpoint is still read once, in full") {
    stubRecords(total = 400, pageSize = 50)
    val perPartition = idsPerPartition(tableWith(None, 50))

    assertEquals(perPartition.length, 1)
    assertEquals(perPartition.head.length, 400)
  }
}

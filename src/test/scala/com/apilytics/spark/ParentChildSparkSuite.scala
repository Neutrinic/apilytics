package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

/** Parent-child joins read end to end through a SparkSession (#277).
  *
  * Batch joins were covered only by config-parsing tests. The loader requires a batch
  * endpoint without a path placeholder, but the table required one to name its parent key
  * column, so every batch-join config the loader accepted failed when the table was built.
  * Nothing planned a query, so nothing noticed.
  */
class ParentChildSparkSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var dir: Path = _

  private val spec =
    """openapi: 3.0.0
      |info: { title: shop, version: "1" }
      |paths:
      |  /customers:
      |    get:
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/json:
      |              schema:
      |                type: array
      |                items:
      |                  type: object
      |                  properties: { id: { type: integer }, name: { type: string } }
      |  /orders:
      |    get:
      |      parameters:
      |        - { name: customer_ids, in: query, schema: { type: string } }
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/json:
      |              schema:
      |                type: array
      |                items:
      |                  type: object
      |                  properties: { order_id: { type: integer }, customer_id: { type: integer }, total: { type: integer } }
      |  /customers/{customer_id}/orders:
      |    get:
      |      parameters:
      |        - { name: customer_id, in: path, required: true, schema: { type: string } }
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/json:
      |              schema:
      |                type: array
      |                items:
      |                  type: object
      |                  properties: { order_id: { type: integer }, total: { type: integer } }
      |""".stripMargin

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()

    server.stubFor(get(urlPathEqualTo("/customers")).willReturn(okJson(
      """[{"id": 1, "name": "ada"}, {"id": 2, "name": "bob"}, {"id": 3, "name": "cy"}]"""
    )))
    // Batch: the API filters by the comma-joined IDs it is sent.
    server.stubFor(get(urlPathEqualTo("/orders")).withQueryParam("customer_ids", equalTo("1,2")).willReturn(okJson(
      """[{"order_id": 10, "customer_id": 1, "total": 5},
        | {"order_id": 11, "customer_id": 1, "total": 7},
        | {"order_id": 20, "customer_id": 2, "total": 9}]""".stripMargin
    )))
    server.stubFor(get(urlPathEqualTo("/orders")).withQueryParam("customer_ids", equalTo("3")).willReturn(okJson(
      """[{"order_id": 30, "customer_id": 3, "total": 4}]"""
    )))
    for ((id, body) <- Seq(1 -> """[{"order_id": 10, "total": 5}]""", 2 -> "[]", 3 -> """[{"order_id": 30, "total": 4}]"""))
      server.stubFor(get(urlPathEqualTo(s"/customers/$id/orders")).willReturn(okJson(body)))

    dir = Files.createTempDirectory("apilytics-parent-child")
    Files.writeString(dir.resolve("shop.yaml"), spec)
    Files.writeString(
      dir.resolve("shop.conf"),
      s"""openapi = "shop.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |tables {
         |  customers { endpoint = "/customers" }
         |  orders_batch {
         |    endpoint        = "/orders"
         |    parent-table    = "customers"
         |    parent-key      = "id"
         |    join-strategy   = "batch"
         |    batch-param     = "customer_ids"
         |    batch-size      = 2
         |    child-key-field = "customer_id"
         |  }
         |  orders_each {
         |    endpoint      = "/customers/{customer_id}/orders"
         |    parent-table  = "customers"
         |    parent-key    = "id"
         |    join-strategy = "nested_loop"
         |  }
         |}
         |""".stripMargin
    )

    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("ParentChildSparkSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", dir.resolve("shop.conf").toAbsolutePath.toString)
      .getOrCreate()
  }

  override def afterEach(context: AfterEach): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    if (server != null) server.stop()
    if (dir != null) Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
  }

  test("a batch join reads every child, matched to its parent, in batches of batch-size") {
    val rows = spark.sql("SELECT _parent_id, order_id FROM api.default.orders_batch")
      .collect().map(r => (r.getString(0), r.getAs[Number](1).longValue)).sorted.toList

    assertEquals(rows, List(("1", 10L), ("1", 11L), ("2", 20L), ("3", 30L)))
    // Three parents with batch-size 2: two calls, not three.
    server.verify(1, getRequestedFor(urlPathEqualTo("/orders")).withQueryParam("customer_ids", equalTo("1,2")))
    server.verify(1, getRequestedFor(urlPathEqualTo("/orders")).withQueryParam("customer_ids", equalTo("3")))
  }

  test("a batch join's parent key column is named after parent-key") {
    val columns = spark.table("api.default.orders_batch").columns.toList
    assertEquals(columns.head, "_parent_id")
  }

  test("a nested-loop join still names its parent key column after the path parameter") {
    val rows = spark.sql("SELECT _parent_customer_id, order_id FROM api.default.orders_each")
      .collect().map(r => (r.getString(0), r.getAs[Number](1).longValue)).sorted.toList

    assertEquals(rows, List(("1", 10L), ("3", 30L)))
  }

  test("a parent-child table in variant mode is refused when loaded, not at read time") {
    // The table advertised a VARIANT column, but the child reader builds Arrow strings, so a
    // query crashed mid-read with "Struct type not supported" (#300). It now fails to load,
    // saying why, while the source's plain tables still read in variant mode.
    val variantConf = dir.resolve("shop-variant.conf")
    Files.writeString(variantConf,
      Files.readString(dir.resolve("shop.conf")).replace("auth { type = none }", "auth { type = none }\nschema { mode = variant }"))
    spark.conf.set("spark.sql.catalog.v", "com.apilytics.spark.RESTCatalog")
    spark.conf.set("spark.sql.catalog.v.config", variantConf.toAbsolutePath.toString)

    val error = intercept[Exception](spark.sql("SELECT value FROM v.default.orders_each").collect())
    assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
      .exists(e => String.valueOf(e.getMessage).contains("variant mode")), s"unexpected failure: $error")

    assertEquals(spark.sql("SELECT value FROM v.default.customers").collect().length, 3)
  }
}

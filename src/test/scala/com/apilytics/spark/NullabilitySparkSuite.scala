package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

/** `IS NULL` finds every null, whatever the spec says about the field (#296).
  *
  * Columns used to be non-nullable for required fields. Spark trusts that: it drops an
  * `IS NULL` filter on a non-nullable column as always false. But a required field can be
  * null in the data, inside an optional parent, when it is also `nullable: true`, or simply
  * because the API breaks its own spec, and those rows vanished from the result.
  */
class NullabilitySparkSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var dir: Path = _

  private val spec =
    """openapi: 3.0.0
      |info: { title: users, version: "1" }
      |paths:
      |  /users:
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
      |                  required: [id, nick, email]
      |                  properties:
      |                    id: { type: integer }
      |                    nick: { type: string, nullable: true }
      |                    email: { type: string }
      |                    profile:
      |                      type: object
      |                      required: [name]
      |                      properties: { name: { type: string } }
      |""".stripMargin

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    // 1: everything set. 2: no profile, null nick. 3: email null, breaking the spec.
    server.stubFor(get(urlPathEqualTo("/users")).willReturn(okJson(
      """[{"id": 1, "nick": "a", "email": "a@x", "profile": {"name": "Ada"}},
        | {"id": 2, "nick": null, "email": "b@x"},
        | {"id": 3, "nick": "c", "email": null, "profile": {"name": "Cy"}}]""".stripMargin)))

    dir = Files.createTempDirectory("apilytics-nullability")
    Files.writeString(dir.resolve("users.yaml"), spec)
    Files.writeString(dir.resolve("users.conf"),
      s"""openapi = "users.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |tables { users { endpoint = "/users" } }
         |""".stripMargin)

    spark = SparkSession.builder().master("local[1]").appName("NullabilitySparkSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", dir.resolve("users.conf").toAbsolutePath.toString)
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

  private def ids(where: String): List[Long] =
    spark.sql(s"SELECT id FROM api.default.users WHERE $where").collect().map(_.getAs[Number](0).longValue).toList.sorted

  test("a required field inside an optional parent is null when the parent is missing") {
    assertEquals(ids("profile_name IS NULL"), List(2L))
  }

  test("a required field declared nullable: true can be null") {
    assertEquals(ids("nick IS NULL"), List(2L))
  }

  test("a required field the API sends as null is still found by IS NULL") {
    assertEquals(ids("email IS NULL"), List(3L))
  }
}

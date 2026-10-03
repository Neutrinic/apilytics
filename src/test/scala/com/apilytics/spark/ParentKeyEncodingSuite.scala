package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

/** A parent key goes into the child's path as one encoded segment (#318). */
class ParentKeyEncodingSuite extends FunSuite {

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
      |                items: { type: object, properties: { login: { type: string } } }
      |  /users/{login}/repos:
      |    get:
      |      parameters:
      |        - { name: login, in: path, required: true, schema: { type: string } }
      |      responses:
      |        "200":
      |          description: ok
      |          content:
      |            application/json:
      |              schema:
      |                type: array
      |                items: { type: object, properties: { repo: { type: string } } }
      |""".stripMargin

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()
    dir = Files.createTempDirectory("apilytics-parent-key")
    Files.writeString(dir.resolve("users.yaml"), spec)
    Files.writeString(dir.resolve("users.conf"),
      s"""openapi = "users.yaml"
         |base-url = "http://localhost:${server.port()}"
         |auth { type = none }
         |http { max-retries = 0, max-backoff = "0 seconds" }
         |tables {
         |  users { endpoint = "/users" }
         |  repos {
         |    endpoint      = "/users/{login}/repos"
         |    parent-table  = "users"
         |    parent-key    = "login"
         |    join-strategy = "nested_loop"
         |  }
         |}
         |""".stripMargin)
    spark = SparkSession.builder().master("local[1]").appName("ParentKeyEncodingSuite")
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

  test("keys with reserved characters are encoded, not parsed as URL syntax") {
    // `x#y` requested /users/x (the rest became a fragment), `p?admin=1` injected a query,
    // `a/b` added a path segment, and `a b` failed the whole query with "Invalid URI". A `..`
    // key has to reach the server encoded, not walk up to /repos.
    server.stubFor(get(urlPathEqualTo("/users")).willReturn(okJson(
      """[{"login": "x#y"}, {"login": "p?admin=1"}, {"login": "a b"}, {"login": "a/b"}, {"login": ".."}, {"login": "plain"}]"""
    )))
    server.stubFor(get(urlPathMatching("/users/.+/repos")).willReturn(okJson("""[{"repo": "r"}]""")))

    val parents = spark.sql("SELECT _parent_login FROM api.default.repos").collect().map(_.getString(0)).toSet

    assertEquals(parents, Set("x#y", "p?admin=1", "a b", "a/b", "..", "plain"))
    val requested = server.getAllServeEvents.asScala.map(_.getRequest.getUrl).filter(_ != "/users").toSet
    assertEquals(requested, Set(
      "/users/x%23y/repos", "/users/p%3Fadmin%3D1/repos", "/users/a%20b/repos", "/users/a%2Fb/repos", "/users/%2E%2E/repos",
      "/users/plain/repos"
    ))
  }

  test("unreserved characters pass through; UTF-8 and dot segments are encoded") {
    import ParentChildUtils.encodePathSegment
    assertEquals(encodePathSegment("plain-1_2.3~"), "plain-1_2.3~")
    assertEquals(encodePathSegment("café"), "caf%C3%A9")
    assertEquals(encodePathSegment(".."), "%2E%2E")
    assertEquals(encodePathSegment("a+b@c:d"), "a%2Bb%40c%3Ad")
  }
}

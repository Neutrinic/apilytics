package com.apilytics.spark

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}

/** OAuth2 client credentials, through Spark's public entry points (#276).
  *
  * The token manager existed and had its own tests, but every Spark reader built its
  * client with `Client.resource`, which never created one. So `oauth2_client` with a
  * `token-url` failed on the first request, asking for a pre-fetched token, and only a
  * test that read through a SparkSession could have seen it. Each test here goes through
  * a different reader: the table scan, `format()`, COUNT pushdown and aggregate pushdown.
  */
class OAuth2SparkReadSuite extends FunSuite {

  private var server: WireMockServer = _
  private var spark: SparkSession = _
  private var configPath: Path = _

  private val issuesPath = "/repos/octocat/Hello-World/issues"
  private val token = "issued-token"

  private val issuesJson =
    """[
      |  {"id": 1, "number": 101, "title": "first",  "state": "open"},
      |  {"id": 2, "number": 102, "title": "second", "state": "closed"},
      |  {"id": 3, "number": 103, "title": "third",  "state": "open"}
      |]""".stripMargin

  override def beforeEach(context: BeforeEach): Unit = {
    server = new WireMockServer(wireMockConfig().dynamicPort())
    server.start()

    server.stubFor(
      post(urlPathEqualTo("/oauth/token"))
        .withRequestBody(containing("grant_type=client_credentials"))
        .willReturn(okJson(s"""{"access_token": "$token", "expires_in": 3600}"""))
    )
    // The API answers only with the issued token; anything else is a 401.
    server.stubFor(any(anyUrl()).atPriority(10).willReturn(unauthorized().withBody("no token")))
    server.stubFor(
      get(urlPathEqualTo(issuesPath)).atPriority(1)
        .withHeader("Authorization", equalTo(s"Bearer $token"))
        .withQueryParam("per_page", absent())
        .willReturn(okJson(issuesJson))
    )
    server.stubFor(
      get(urlPathEqualTo(issuesPath)).atPriority(1)
        .withHeader("Authorization", equalTo(s"Bearer $token"))
        .withQueryParam("per_page", equalTo("1"))
        .willReturn(okJson("""{"total_count": 42, "items": []}"""))
    )
    server.stubFor(
      get(urlPathEqualTo("/stats")).atPriority(1)
        .withHeader("Authorization", equalTo(s"Bearer $token"))
        .willReturn(okJson("""{"total": 306}"""))
    )

    val spec = Path.of("src/test/resources/github-issues.yaml").toAbsolutePath.toString.replace("\\", "/")
    configPath = Files.createTempFile("apilytics-oauth2", ".conf")
    Files.writeString(
      configPath,
      s"""openapi = "$spec"
         |base-url = "http://localhost:${server.port()}"
         |auth {
         |  type = oauth2_client
         |  client-id = "test-client"
         |  client-secret = "test-secret"
         |  token-url = "http://localhost:${server.port()}/oauth/token"
         |}
         |tables {
         |  issues {
         |    endpoint = "$issuesPath"
         |  }
         |  issue_count {
         |    endpoint = "$issuesPath"
         |    count { param = "per_page", param-value = "1", response-path = "/total_count" }
         |  }
         |  issue_sum {
         |    endpoint = "$issuesPath"
         |    aggregations { total { function = "sum", column = "number", endpoint = "/stats", response-path = "/total" } }
         |  }
         |}
         |""".stripMargin
    )

    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("OAuth2SparkReadSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
      .config("spark.sql.catalog.api.config", configPath.toAbsolutePath.toString)
      .getOrCreate()
  }

  override def afterEach(context: AfterEach): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    if (server != null) server.stop()
    if (configPath != null) Files.deleteIfExists(configPath)
  }

  private def tokenRequests: Int =
    server.findAll(postRequestedFor(urlPathEqualTo("/oauth/token"))).size

  test("a catalog scan fetches a token and sends it") {
    val titles = spark.sql("SELECT title FROM api.default.issues").collect().map(_.getString(0)).sorted

    assertEquals(titles.toList, List("first", "second", "third"))
    assert(tokenRequests >= 1, "the token endpoint was never called")
  }

  test("format() fetches a token and sends it") {
    val rows = spark.read.format("apilytics")
      .option("config", configPath.toAbsolutePath.toString)
      .option("table", "issues")
      .load()
      .count()

    assertEquals(rows, 3L)
    assert(tokenRequests >= 1, "the token endpoint was never called")
  }

  test("COUNT pushdown fetches a token and sends it") {
    val rows = spark.sql("SELECT COUNT(*) FROM api.default.issue_count").collect()

    assertEquals(rows.head.getLong(0), 42L)
    assert(tokenRequests >= 1, "the token endpoint was never called")
  }

  test("aggregate pushdown fetches a token and sends it") {
    val rows = spark.sql("SELECT SUM(number) FROM api.default.issue_sum").collect()

    assertEquals(rows.head.getLong(0), 306L)
    server.verify(0, getRequestedFor(urlPathEqualTo(issuesPath)))
    assert(tokenRequests >= 1, "the token endpoint was never called")
  }
}

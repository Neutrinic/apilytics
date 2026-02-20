package com.apilytics.e2e

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.Converter
import com.apilytics.core.config._
import com.apilytics.core.http.{Client, Paginator}
import com.apilytics.core.openapi.{Endpoint, OpenAPISchema, Parser}
import com.apilytics.core.schema.SchemaMapper
import munit.FunSuite
import org.apache.arrow.memory.RootAllocator
import org.http4s.Uri

import java.nio.charset.StandardCharsets
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class GitHubE2ESuite extends FunSuite {

  private val e2eEnabled = sys.env.contains("E2E")

  private val specPath = java.nio.file.Paths.get(getClass.getClassLoader.getResource("github-issues.yaml").toURI).toString
  private val httpConfig = HttpConfig(maxRetries = 1, maxBackoff = 5.seconds, timeout = 10.seconds)
  private val githubAuth = sys.env.get("GITHUB_TOKEN").fold(
    AuthConfig(authType = AuthType.Header, headerName = Some("Accept"), headerValue = Some("application/vnd.github+json"))
  ) { token =>
    AuthConfig(authType = AuthType.Bearer, token = Some(token))
  }
  private val pagination = PaginationConfig(style = PaginationStyle.LinkHeader, pageSizeParam = Some("per_page"), maxPageSize = 5)

  /** The array response gets wrapped by Parser as ObjectType(Map("data" -> ArrayType(itemSchema))).
    * Extract the item ObjectType for Arrow schema mapping. */
  private def itemSchema(ep: Endpoint): OpenAPISchema.ObjectType = {
    ep.responseSchema.properties("data") match {
      case OpenAPISchema.ArrayType(obj: OpenAPISchema.ObjectType) => obj
      case other => throw new IllegalStateException(s"Expected ArrayType(ObjectType) but got $other")
    }
  }

  test("parse spec discovers issues endpoint".tag(new munit.Tag("e2e"))) {
    assume(e2eEnabled, "Set E2E=1 to run")
    val parsed = Parser.parse(specPath)
    assertEquals(parsed.baseUrl, "https://api.github.com")
    val ep = parsed.endpoints.find(_.path == "/repos/{owner}/{repo}/issues")
    assert(ep.isDefined, "Expected issues endpoint")
    assert(ep.get.queryParams.exists(_.name == "state"))
  }

  test("schema produces expected columns".tag(new munit.Tag("e2e"))) {
    assume(e2eEnabled, "Set E2E=1 to run")
    val parsed = Parser.parse(specPath)
    val ep = parsed.endpoints.find(_.path == "/repos/{owner}/{repo}/issues").get
    val arrowSchema = SchemaMapper.toArrowSchema(itemSchema(ep))
    val fieldNames = arrowSchema.getFields.asScala.map(_.getName).toSet
    assert(fieldNames.contains("id"), s"Missing 'id' in $fieldNames")
    assert(fieldNames.contains("title"), s"Missing 'title' in $fieldNames")
    assert(fieldNames.contains("state"), s"Missing 'state' in $fieldNames")
    assert(fieldNames.contains("user_login"), s"Missing 'user_login' in $fieldNames")
  }

  test("fetch issues from octocat/Hello-World".tag(new munit.Tag("e2e"))) {
    assume(e2eEnabled, "Set E2E=1 to run")
    val parsed = Parser.parse(specPath)
    val ep = parsed.endpoints.find(_.path == "/repos/{owner}/{repo}/issues").get
    val arrowSchema = SchemaMapper.toArrowSchema(itemSchema(ep))
    val allocator = new RootAllocator()

    try {
      val baseUri = Uri.unsafeFromString("https://api.github.com/repos/octocat/Hello-World/issues")
      val params = Map("state" -> "open")
      val rows = Client.resource(httpConfig, githubAuth).use { client =>
        Paginator.pages(client, baseUri, params, pagination, limit = Some(5))
          .map(_._1).compile.toList.map { pages =>
            assert(pages.nonEmpty, "Expected at least one page")
            pages.flatMap { pageJson =>
              val records = Converter.extractRecords(pageJson, None)
              if (records.isEmpty) Nil
              else {
                val root = Converter.toArrow(records, arrowSchema, allocator)
                try {
                  val rowCount = root.getRowCount
                  val titleVec = root.getVector("title").asInstanceOf[org.apache.arrow.vector.VarCharVector]
                  val stateVec = root.getVector("state").asInstanceOf[org.apache.arrow.vector.VarCharVector]
                  (0 until rowCount).map { i =>
                    val title = new String(titleVec.get(i), StandardCharsets.UTF_8)
                    val state = new String(stateVec.get(i), StandardCharsets.UTF_8)
                    (title, state)
                  }.toList
                } finally root.close()
              }
            }
          }
      }.unsafeRunSync()

      assert(rows.nonEmpty, "Expected at least one issue")
      rows.foreach { case (_, state) =>
        assertEquals(state, "open")
      }
    } finally allocator.close()
  }

  test("link header pagination fetches multiple pages".tag(new munit.Tag("e2e"))) {
    assume(e2eEnabled, "Set E2E=1 to run")
    val baseUri = Uri.unsafeFromString("https://api.github.com/repos/octocat/Hello-World/issues")
    val smallPage = pagination.copy(maxPageSize = 2)

    val pages = Client.resource(httpConfig, githubAuth).use { client =>
      Paginator.pages(client, baseUri, Map("state" -> "all"), smallPage, limit = Some(4))
        .map(_._1).compile.toList
    }.unsafeRunSync()

    assert(pages.size >= 2, s"Expected at least 2 pages but got ${pages.size}")
  }
}

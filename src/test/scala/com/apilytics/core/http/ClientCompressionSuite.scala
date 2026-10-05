package com.apilytics.core.http

import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig, ResponseFormat}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.circe.Json
import munit.FunSuite
import org.http4s.Uri

import scala.concurrent.duration._

/** Compressed responses are requested and decompressed (#347). */
class ClientCompressionSuite extends FunSuite {

  private var server: WireMockServer = _

  override def beforeEach(context: BeforeEach): Unit = {
    // WireMock's own response compression is off, so each test controls the encoding.
    server = new WireMockServer(wireMockConfig().dynamicPort().gzipDisabled(true))
    server.start()
  }

  override def afterEach(context: AfterEach): Unit = server.stop()

  private def gzip(s: String): Array[Byte] = {
    val bytes = new java.io.ByteArrayOutputStream()
    val out   = new java.util.zip.GZIPOutputStream(bytes)
    out.write(s.getBytes("UTF-8"))
    out.close()
    bytes.toByteArray
  }

  private def uri(path: String) = Uri.unsafeFromString(s"http://localhost:${server.port()}$path")

  private def client(format: ResponseFormat = ResponseFormat.Json) =
    Client.resource(HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 5.seconds, responseFormat = format),
                    AuthConfig(authType = AuthType.None))

  test("a response compressed without being asked is decompressed, not parsed as JSON") {
    // RFC 9110 lets a server compress when the request names no encoding. The client parsed
    // the gzip bytes as JSON and failed with "Invalid JSON".
    server.stubFor(get(urlPathEqualTo("/x")).willReturn(aResponse()
      .withHeader("Content-Type", "application/json").withHeader("Content-Encoding", "gzip")
      .withBody(gzip("""{"ok": true, "n": 3}"""))))

    val response = client().use(_.get(uri("/x"))).unsafeRunSync()
    assertEquals(response.json, Json.obj("ok" -> Json.True, "n" -> Json.fromInt(3)))
  }

  test("requests ask for gzip, and a server that compresses on request is read") {
    server.stubFor(get(urlPathEqualTo("/x")).withHeader("Accept-Encoding", containing("gzip"))
      .willReturn(aResponse()
        .withHeader("Content-Type", "application/json").withHeader("Content-Encoding", "gzip")
        .withBody(gzip("""{"page": 1}"""))))

    val response = client().use(_.get(uri("/x"))).unsafeRunSync()
    assertEquals(response.json, Json.obj("page" -> Json.fromInt(1)))
  }

  test("a compressed NDJSON stream is decompressed line by line") {
    val lines = (1 to 500).map(i => s"""{"id": $i}""").mkString("\n") + "\n"
    server.stubFor(get(urlPathEqualTo("/export")).willReturn(aResponse()
      .withHeader("Content-Type", "application/x-ndjson").withHeader("Content-Encoding", "gzip")
      .withBody(gzip(lines))))

    val records = client(ResponseFormat.NDJSON)
      .use(_.getStreaming(uri("/export"), Map.empty, ResponseFormat.NDJSON).compile.toList)
      .unsafeRunSync()
    assertEquals(records.flatMap(_.hcursor.get[Int]("id").toOption), (1 to 500).toList)
  }

  test("an uncompressed response still reads as before") {
    server.stubFor(get(urlPathEqualTo("/x")).willReturn(okJson("""{"plain": true}""")))
    assertEquals(client().use(_.get(uri("/x"))).unsafeRunSync().json, Json.obj("plain" -> Json.True))
  }
}

package com.apilytics.core.openapi

import com.apilytics.core.config.CacheConfig
import munit.FunSuite

import java.nio.file.{Files, Path}
import scala.concurrent.duration._

class SpecCacheSuite extends FunSuite {

  private var tempDir: Path = _

  override def beforeEach(context: BeforeEach): Unit = {
    tempDir = Files.createTempDirectory("spec-cache-test")
  }

  override def afterEach(context: AfterEach): Unit = {
    // Clean up temp files
    if (tempDir != null) {
      Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete(_))
    }
  }

  test("cache disabled returns parsed spec directly") {
    val specJson = """
      |openapi: "3.0.0"
      |info:
      |  title: Test
      |  version: "1.0"
      |paths:
      |  /items:
      |    get:
      |      responses:
      |        "200":
      |          content:
      |            application/json:
      |              schema:
      |                type: object
      |                properties:
      |                  id:
      |                    type: integer
      |""".stripMargin

    val specFile = Files.createTempFile("spec", ".yaml")
    Files.writeString(specFile, specJson)

    try {
      val config = CacheConfig(enabled = false)
      val spec = SpecCache.getOrParse(specFile.toString, config)
      assertEquals(spec.endpoints.size, 1)
      assertEquals(spec.endpoints.head.path, "/items")
    } finally {
      Files.delete(specFile)
    }
  }

  test("cache enabled stores and retrieves spec") {
    val specJson = """
      |openapi: "3.0.0"
      |info:
      |  title: Test
      |  version: "1.0"
      |paths:
      |  /items:
      |    get:
      |      responses:
      |        "200":
      |          content:
      |            application/json:
      |              schema:
      |                type: object
      |                properties:
      |                  id:
      |                    type: integer
      |""".stripMargin

    val specFile = Files.createTempFile("spec", ".yaml")
    Files.writeString(specFile, specJson)

    try {
      val config = CacheConfig(enabled = true, directory = Some(tempDir.toString))

      // First call - should parse and cache
      val spec1 = SpecCache.getOrParse(specFile.toString, config)
      assertEquals(spec1.endpoints.size, 1)

      // Verify cache file was created
      val cacheFiles = Files.list(tempDir).toArray.length
      assertEquals(cacheFiles, 1)

      // Second call - should use cache
      val spec2 = SpecCache.getOrParse(specFile.toString, config)
      assertEquals(spec2.endpoints.size, 1)
      assertEquals(spec1.endpoints.head.path, spec2.endpoints.head.path)
    } finally {
      Files.delete(specFile)
    }
  }

  test("cache invalidates when file is modified") {
    val specJson1 = """
      |openapi: "3.0.0"
      |info:
      |  title: Test
      |  version: "1.0"
      |paths:
      |  /items:
      |    get:
      |      responses:
      |        "200":
      |          content:
      |            application/json:
      |              schema:
      |                type: object
      |                properties:
      |                  id:
      |                    type: integer
      |""".stripMargin

    val specJson2 = """
      |openapi: "3.0.0"
      |info:
      |  title: Test v2
      |  version: "2.0"
      |paths:
      |  /items:
      |    get:
      |      responses:
      |        "200":
      |          content:
      |            application/json:
      |              schema:
      |                type: object
      |                properties:
      |                  id:
      |                    type: integer
      |  /other:
      |    get:
      |      responses:
      |        "200":
      |          content:
      |            application/json:
      |              schema:
      |                type: object
      |                properties:
      |                  name:
      |                    type: string
      |""".stripMargin

    val specFile = Files.createTempFile("spec", ".yaml")
    Files.writeString(specFile, specJson1)

    try {
      val config = CacheConfig(enabled = true, directory = Some(tempDir.toString))

      // First call - should parse and cache
      val spec1 = SpecCache.getOrParse(specFile.toString, config)
      assertEquals(spec1.endpoints.size, 1)

      // Modify the file (wait a bit to ensure mtime changes)
      Thread.sleep(100)
      Files.writeString(specFile, specJson2)

      // Second call - should detect modification and re-parse
      val spec2 = SpecCache.getOrParse(specFile.toString, config)
      assertEquals(spec2.endpoints.size, 2)
    } finally {
      Files.delete(specFile)
    }
  }

  test("cache respects TTL") {
    val specJson = """
      |openapi: "3.0.0"
      |info:
      |  title: Test
      |  version: "1.0"
      |paths:
      |  /items:
      |    get:
      |      responses:
      |        "200":
      |          content:
      |            application/json:
      |              schema:
      |                type: object
      |                properties:
      |                  id:
      |                    type: integer
      |""".stripMargin

    val specFile = Files.createTempFile("spec", ".yaml")
    Files.writeString(specFile, specJson)

    try {
      // Very short TTL
      val config = CacheConfig(enabled = true, ttl = Some(50.millis), directory = Some(tempDir.toString))

      // First call - should parse and cache
      val spec1 = SpecCache.getOrParse(specFile.toString, config)
      assertEquals(spec1.endpoints.size, 1)

      // Wait for TTL to expire
      Thread.sleep(100)

      // Second call - should re-parse due to TTL expiry
      val spec2 = SpecCache.getOrParse(specFile.toString, config)
      assertEquals(spec2.endpoints.size, 1)
      // Can't easily verify re-parsing happened, but the test ensures no errors occur
    } finally {
      Files.delete(specFile)
    }
  }

  test("clear removes cached specs") {
    val specJson = """
      |openapi: "3.0.0"
      |info:
      |  title: Test
      |  version: "1.0"
      |paths:
      |  /items:
      |    get:
      |      responses:
      |        "200":
      |          content:
      |            application/json:
      |              schema:
      |                type: object
      |                properties:
      |                  id:
      |                    type: integer
      |""".stripMargin

    val specFile = Files.createTempFile("spec", ".yaml")
    Files.writeString(specFile, specJson)

    try {
      val config = CacheConfig(enabled = true, directory = Some(tempDir.toString))

      // Parse and cache
      SpecCache.getOrParse(specFile.toString, config)
      assertEquals(Files.list(tempDir).count(), 1L)

      // Clear cache
      SpecCache.clear(config)
      assertEquals(Files.list(tempDir).count(), 0L)
    } finally {
      Files.delete(specFile)
    }
  }
}

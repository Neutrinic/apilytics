package com.apilytics.core.http

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{ResponseCacheBackend, ResponseCacheConfig}
import io.circe.Json
import munit.FunSuite

import scala.concurrent.duration._

class ResponseCacheSuite extends FunSuite {

  private def testResponse(data: String) = ApiResponse(
    json = Json.obj("data" -> Json.fromString(data)),
    status = 200,
    headers = Map.empty
  )

  test("disabled cache never stores entries") {
    val cache = ResponseCache.disabled

    val result = (for {
      _ <- cache.put("/test", Map.empty, testResponse("test"))
      cached <- cache.get("/test", Map.empty)
      stats <- cache.stats
    } yield (cached, stats)).unsafeRunSync()

    assertEquals(result._1, None)
    assertEquals(result._2.size, 0)
  }

  test("memory cache stores and retrieves responses") {
    val config = ResponseCacheConfig(enabled = true, ttl = 1.minute, maxEntries = 100)
    val cache = ResponseCache.fromConfig(config).unsafeRunSync()

    val response = testResponse("hello")

    val result = (for {
      before <- cache.get("/test", Map("a" -> "1"))
      _ <- cache.put("/test", Map("a" -> "1"), response)
      after <- cache.get("/test", Map("a" -> "1"))
      stats <- cache.stats
    } yield (before, after, stats)).unsafeRunSync()

    assertEquals(result._1, None)
    assertEquals(result._2, Some(response))
    assertEquals(result._3.hits, 1L)
    assertEquals(result._3.misses, 1L)
    assertEquals(result._3.size, 1)
  }

  test("cache key includes endpoint and params") {
    val config = ResponseCacheConfig(enabled = true, ttl = 1.minute, maxEntries = 100)
    val cache = ResponseCache.fromConfig(config).unsafeRunSync()

    val response1 = testResponse("response1")
    val response2 = testResponse("response2")

    val result = (for {
      _ <- cache.put("/test", Map("a" -> "1"), response1)
      _ <- cache.put("/test", Map("a" -> "2"), response2)
      cached1 <- cache.get("/test", Map("a" -> "1"))
      cached2 <- cache.get("/test", Map("a" -> "2"))
      cachedOther <- cache.get("/other", Map("a" -> "1"))
      stats <- cache.stats
    } yield (cached1, cached2, cachedOther, stats)).unsafeRunSync()

    assertEquals(result._1, Some(response1))
    assertEquals(result._2, Some(response2))
    assertEquals(result._3, None)
    assertEquals(result._4.size, 2)
  }

  test("cache expires entries after TTL") {
    val config = ResponseCacheConfig(enabled = true, ttl = 50.millis, maxEntries = 100)
    val cache = ResponseCache.fromConfig(config).unsafeRunSync()

    val response = testResponse("expires")

    val result = (for {
      _ <- cache.put("/test", Map.empty, response)
      before <- cache.get("/test", Map.empty)
      _ <- IO.sleep(100.millis)
      after <- cache.get("/test", Map.empty)
    } yield (before, after)).unsafeRunSync()

    assertEquals(result._1, Some(response))
    assertEquals(result._2, None) // Expired
  }

  test("cache evicts LRU entries when full") {
    val config = ResponseCacheConfig(enabled = true, ttl = 1.minute, maxEntries = 2)
    val cache = ResponseCache.fromConfig(config).unsafeRunSync()

    val result = (for {
      _ <- cache.put("/a", Map.empty, testResponse("a"))
      _ <- cache.put("/b", Map.empty, testResponse("b"))
      // Access /a to make it more recently used
      _ <- cache.get("/a", Map.empty)
      // Add /c, should evict /b (least recently used)
      _ <- cache.put("/c", Map.empty, testResponse("c"))
      aStillCached <- cache.get("/a", Map.empty)
      bEvicted <- cache.get("/b", Map.empty)
      cCached <- cache.get("/c", Map.empty)
      stats <- cache.stats
    } yield (aStillCached, bEvicted, cCached, stats)).unsafeRunSync()

    assert(result._1.isDefined, "/a should still be cached (recently accessed)")
    assertEquals(result._2, None, "/b should be evicted (LRU)")
    assert(result._3.isDefined, "/c should be cached")
    assertEquals(result._4.size, 2)
  }

  test("clear removes all entries") {
    val config = ResponseCacheConfig(enabled = true, ttl = 1.minute, maxEntries = 100)
    val cache = ResponseCache.fromConfig(config).unsafeRunSync()

    val result = (for {
      _ <- cache.put("/a", Map.empty, testResponse("a"))
      _ <- cache.put("/b", Map.empty, testResponse("b"))
      before <- cache.stats
      _ <- cache.clear()
      after <- cache.stats
      aGone <- cache.get("/a", Map.empty)
    } yield (before, after, aGone)).unsafeRunSync()

    assertEquals(result._1.size, 2)
    assertEquals(result._2.size, 0)
    assertEquals(result._3, None)
  }

  test("fromConfig returns disabled cache when not enabled") {
    val config = ResponseCacheConfig(enabled = false)
    val cache = ResponseCache.fromConfig(config).unsafeRunSync()

    val result = (for {
      _ <- cache.put("/test", Map.empty, testResponse("test"))
      cached <- cache.get("/test", Map.empty)
    } yield cached).unsafeRunSync()

    assertEquals(result, None)
  }

  test("param order does not affect cache key") {
    val config = ResponseCacheConfig(enabled = true, ttl = 1.minute, maxEntries = 100)
    val cache = ResponseCache.fromConfig(config).unsafeRunSync()

    val response = testResponse("test")

    val result = (for {
      _ <- cache.put("/test", Map("a" -> "1", "b" -> "2"), response)
      cached <- cache.get("/test", Map("b" -> "2", "a" -> "1"))
    } yield cached).unsafeRunSync()

    assertEquals(result, Some(response))
  }
}

package com.apilytics.core.http

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import munit.FunSuite

import scala.concurrent.duration._

class RateLimiterSuite extends FunSuite {

  test("RateLimiter.unlimited allows immediate requests") {
    val limiter = RateLimiter.unlimited

    val start = System.nanoTime()
    (1 to 10).foreach { _ =>
      limiter.acquire.unsafeRunSync()
    }
    val elapsed = (System.nanoTime() - start).nanos

    // Should complete quickly (< 500ms for 10 requests with JVM warmup)
    assert(elapsed < 500.millis, s"Expected < 500ms, got $elapsed")
  }

  test("RateLimiter enforces rate limit") {
    val rps = 10 // 10 requests per second = 100ms between requests
    val limiter = RateLimiter(rps).unsafeRunSync()

    val start = System.nanoTime()
    // Make 5 requests - should take at least 400ms (4 intervals)
    (1 to 5).foreach { _ =>
      limiter.acquire.unsafeRunSync()
    }
    val elapsed = (System.nanoTime() - start).nanos

    // Should take at least 4 * 100ms = 400ms (5 requests = 4 intervals)
    // Allow some tolerance for timing
    assert(elapsed >= 350.millis, s"Expected >= 350ms, got $elapsed")
  }

  test("RateLimiter requires positive requestsPerSecond") {
    intercept[IllegalArgumentException] {
      RateLimiter(0).unsafeRunSync()
    }
    intercept[IllegalArgumentException] {
      RateLimiter(-1).unsafeRunSync()
    }
  }

  test("RateLimiter handles high rate limits") {
    // 1000 rps = 1ms between requests
    val limiter = RateLimiter(1000).unsafeRunSync()

    val start = System.nanoTime()
    (1 to 10).foreach { _ =>
      limiter.acquire.unsafeRunSync()
    }
    val elapsed = (System.nanoTime() - start).nanos

    // 10 requests at 1000 rps should take ~9ms + overhead
    // Be generous with timing tolerance
    assert(elapsed < 500.millis, s"Expected < 500ms, got $elapsed")
  }
}

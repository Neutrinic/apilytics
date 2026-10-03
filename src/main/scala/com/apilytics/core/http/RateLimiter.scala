package com.apilytics.core.http

import cats.effect.{IO, Ref}
import cats.effect.std.Semaphore

import scala.concurrent.duration._

/** Fixed-interval rate limiter for controlling request rate.
  *
  * Enforces a minimum interval between requests. Unlike a token bucket, this does
  * NOT allow bursts — if the API was idle for 30 seconds, the next request still
  * waits for the interval from the previous one. This is intentional for API rate
  * limiting where bursts can trigger 429s.
  *
  * Note: The semaphore serializes all requests through acquire, even if callers
  * attempt concurrent access. This is intentional for single-partition scans.
  * If parallel partition reads are added later, this becomes a bottleneck.
  *
  * Every attempt takes a permit: retries after a 429, a 5xx or a network error, and the
  * re-issue after an OAuth2 token refresh, count against the limit like any request (#279).
  */
trait RateLimiter {
  /** Acquire a permit to make a request. Blocks until rate limit allows. */
  def acquire: IO[Unit]
}

object RateLimiter {

  /** Create a rate limiter that enforces `requestsPerSecond` requests per second.
    *
    * Each request is spaced at least `1/requestsPerSecond` seconds apart.
    * The interval calculation truncates, so the actual rate may be slightly
    * higher than requested (e.g., 3 rps gives 333ms intervals = ~3.003 rps).
    */
  def apply(requestsPerSecond: Int): IO[RateLimiter] = {
    require(requestsPerSecond > 0, "requestsPerSecond must be positive")

    val intervalNanos = 1_000_000_000L / requestsPerSecond

    for {
      lastRequest <- Ref.of[IO, Long](0L)
      semaphore <- Semaphore[IO](1)
    } yield new RateLimiter {
      override def acquire: IO[Unit] = {
        semaphore.permit.use { _ =>
          for {
            now <- IO.realTime.map(_.toNanos)
            last <- lastRequest.get
            waitTime = {
              val elapsed = now - last
              val remaining = intervalNanos - elapsed
              if (remaining > 0) remaining.nanos else Duration.Zero
            }
            _ <- if (waitTime > Duration.Zero) IO.sleep(waitTime) else IO.unit
            actualNow <- IO.realTime.map(_.toNanos)
            _ <- lastRequest.set(actualNow)
          } yield ()
        }
      }
    }
  }

  /** A no-op rate limiter that doesn't limit anything. */
  val unlimited: RateLimiter = new RateLimiter {
    override def acquire: IO[Unit] = IO.unit
  }
}

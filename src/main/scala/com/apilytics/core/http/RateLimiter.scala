package com.apilytics.core.http

import cats.effect.{IO, Ref}
import cats.effect.std.Semaphore

import scala.concurrent.duration._

/** Token bucket rate limiter for controlling request rate.
  *
  * Uses a simple token bucket algorithm:
  * - Bucket has capacity equal to requestsPerSecond (allows small bursts)
  * - Tokens are added at a rate of requestsPerSecond per second
  * - Each request consumes one token
  * - If no tokens available, waits until one becomes available
  */
trait RateLimiter {
  /** Acquire a permit to make a request. Blocks until rate limit allows. */
  def acquire: IO[Unit]
}

object RateLimiter {

  /** Create a rate limiter that allows up to `requestsPerSecond` requests per second.
    *
    * The limiter uses a token bucket with capacity equal to requestsPerSecond,
    * allowing small bursts while maintaining the average rate over time.
    */
  def apply(requestsPerSecond: Int): IO[RateLimiter] = {
    require(requestsPerSecond > 0, "requestsPerSecond must be positive")

    val intervalNanos = (1_000_000_000L / requestsPerSecond)

    for {
      lastRequest <- Ref.of[IO, Long](0L)
      semaphore <- Semaphore[IO](1) // Ensure only one request calculates wait time at a time
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

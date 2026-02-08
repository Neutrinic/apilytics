package com.apilytics.core.http

import cats.effect.{IO, Ref}
import com.apilytics.core.config.ResponseCacheConfig
import io.circe.Json
import org.slf4j.LoggerFactory

import java.security.MessageDigest
import scala.concurrent.duration.FiniteDuration

/** Cache for API responses to avoid redundant requests during development/testing. */
trait ResponseCache {
  /** Get a cached response, if present and not expired. */
  def get(endpoint: String, params: Map[String, String]): IO[Option[ApiResponse]]

  /** Store a response in the cache. */
  def put(endpoint: String, params: Map[String, String], response: ApiResponse): IO[Unit]

  /** Clear all cached entries. */
  def clear(): IO[Unit]

  /** Get cache statistics (hits, misses, size). */
  def stats: IO[ResponseCache.Stats]
}

object ResponseCache {

  final case class Stats(hits: Long, misses: Long, size: Int)

  /** No-op cache that never stores anything. */
  val disabled: ResponseCache = new ResponseCache {
    def get(endpoint: String, params: Map[String, String]): IO[Option[ApiResponse]] = IO.pure(None)
    def put(endpoint: String, params: Map[String, String], response: ApiResponse): IO[Unit] = IO.unit
    def clear(): IO[Unit] = IO.unit
    def stats: IO[Stats] = IO.pure(Stats(0, 0, 0))
  }

  /** Create a cache based on configuration. */
  def fromConfig(config: ResponseCacheConfig): IO[ResponseCache] = {
    if (!config.enabled) IO.pure(disabled)
    else MemoryCache(config.ttl, config.maxEntries)
  }

  /** In-memory LRU cache with TTL expiration. */
  private[http] object MemoryCache {
    private val log = LoggerFactory.getLogger(getClass)

    private case class Entry(
        response: ApiResponse,
        expiresAt: Long
    )

    private case class State(
        cache: Map[String, Entry],
        accessOrder: List[String], // Most recently accessed first
        hits: Long,
        misses: Long
    )

    def apply(ttl: FiniteDuration, maxEntries: Int): IO[ResponseCache] = {
      Ref.of[IO, State](State(Map.empty, Nil, 0, 0)).map { ref =>
        new ResponseCache {
          private def cacheKey(endpoint: String, params: Map[String, String]): String = {
            val sortedParams = params.toList.sorted.map { case (k, v) => s"$k=$v" }.mkString("&")
            val input = s"$endpoint?$sortedParams"
            val digest = MessageDigest.getInstance("SHA-256")
            digest.digest(input.getBytes("UTF-8")).map("%02x".format(_)).mkString
          }

          def get(endpoint: String, params: Map[String, String]): IO[Option[ApiResponse]] = {
            val key = cacheKey(endpoint, params)
            val now = System.currentTimeMillis()

            ref.modify { state =>
              state.cache.get(key) match {
                case Some(entry) if entry.expiresAt > now =>
                  // Cache hit - move to front of access order
                  val newOrder = key :: state.accessOrder.filterNot(_ == key)
                  val newState = state.copy(
                    accessOrder = newOrder,
                    hits = state.hits + 1
                  )
                  log.debug("Cache HIT for {} (key={})", endpoint, key.take(8))
                  (newState, Some(entry.response))

                case Some(_) =>
                  // Entry expired - remove it
                  val newState = state.copy(
                    cache = state.cache - key,
                    accessOrder = state.accessOrder.filterNot(_ == key),
                    misses = state.misses + 1
                  )
                  log.debug("Cache EXPIRED for {} (key={})", endpoint, key.take(8))
                  (newState, None)

                case None =>
                  log.debug("Cache MISS for {} (key={})", endpoint, key.take(8))
                  (state.copy(misses = state.misses + 1), None)
              }
            }
          }

          def put(endpoint: String, params: Map[String, String], response: ApiResponse): IO[Unit] = {
            val key = cacheKey(endpoint, params)
            val now = System.currentTimeMillis()
            val expiresAt = now + ttl.toMillis

            ref.update { state =>
              // Remove expired entries first
              val unexpired = state.cache.filter { case (_, entry) => entry.expiresAt > now }
              val unexpiredKeys = unexpired.keySet

              // Add new entry
              val withNew = unexpired + (key -> Entry(response, expiresAt))
              val newOrder = key :: state.accessOrder.filter(unexpiredKeys.contains).filterNot(_ == key)

              // Evict LRU entries if over max
              val (finalCache, finalOrder) = if (withNew.size > maxEntries) {
                val toEvict = newOrder.drop(maxEntries).toSet
                log.debug("Evicting {} entries from cache", toEvict.size)
                (withNew -- toEvict, newOrder.take(maxEntries))
              } else {
                (withNew, newOrder)
              }

              log.debug("Cache PUT for {} (key={}, size={})", endpoint, key.take(8), finalCache.size)
              state.copy(cache = finalCache, accessOrder = finalOrder)
            }
          }

          def clear(): IO[Unit] = {
            ref.update(state => State(Map.empty, Nil, state.hits, state.misses)).flatMap { _ =>
              IO(log.info("Response cache cleared"))
            }
          }

          def stats: IO[Stats] = ref.get.map { state =>
            Stats(state.hits, state.misses, state.cache.size)
          }
        }
      }
    }
  }
}

package com.apilytics.core.http

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.ResponseCacheConfig
import io.circe.Json
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration

/** Cache for API responses to avoid redundant requests during development/testing.
  *
  * WARNING: Response caching can cause inconsistent results with paginated queries.
  * When pagination is enabled, the first page may be cached while subsequent pages
  * are fetched fresh. Only enable caching for development/testing workflows where
  * you understand this trade-off.
  */
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

  /** Get or create a cache based on configuration.
    *
    * Returns a singleton cache per JVM (executor) that persists across queries.
    * This is important for the cache to actually be useful - a per-scan cache
    * would start empty each time and never benefit from caching.
    *
    * In distributed Spark, each executor gets its own cache instance, which still
    * reduces redundant API calls from that executor.
    */
  def fromConfig(config: ResponseCacheConfig): ResponseCache = synchronized {
    if (!config.enabled) disabled
    else {
      // Use singleton cache per config (keyed by ttl + maxEntries for identity)
      val key = (config.ttl, config.maxEntries)
      singletonCaches.getOrElseUpdate(key, {
        log.info("Creating response cache (ttl={}, maxEntries={})", config.ttl, config.maxEntries)
        MemoryCache(config.ttl, config.maxEntries)
      })
    }
  }

  /** Clear singleton caches (for testing). */
  private[http] def clearSingletons(): Unit = synchronized {
    singletonCaches.clear()
  }

  private val log = LoggerFactory.getLogger(getClass)

  // Singleton cache instances per executor, keyed by config params
  private val singletonCaches = scala.collection.mutable.Map[(FiniteDuration, Int), ResponseCache]()

  /** In-memory LRU cache with TTL expiration. Thread-safe via Ref. */
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

    def apply(ttl: FiniteDuration, maxEntries: Int): ResponseCache = {
      // Create a shared Ref that persists for the cache lifetime
      val ref = Ref.unsafe[IO, State](State(Map.empty, Nil, 0, 0))

      new ResponseCache {
        private def cacheKey(endpoint: String, params: Map[String, String]): String = {
          // Use simple string key for debuggability instead of SHA-256
          val sortedParams = params.toList.sorted.map { case (k, v) => s"$k=$v" }.mkString("&")
          if (sortedParams.isEmpty) endpoint else s"$endpoint?$sortedParams"
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
                log.debug("Cache HIT: {}", key)
                (newState, Some(entry.response))

              case Some(_) =>
                // Entry expired - remove it
                val newState = state.copy(
                  cache = state.cache - key,
                  accessOrder = state.accessOrder.filterNot(_ == key),
                  misses = state.misses + 1
                )
                log.debug("Cache EXPIRED: {}", key)
                (newState, None)

              case None =>
                log.debug("Cache MISS: {}", key)
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

            log.debug("Cache PUT: {} (size={})", key, finalCache.size)
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

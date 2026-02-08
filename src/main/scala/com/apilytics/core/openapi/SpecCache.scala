package com.apilytics.core.openapi

import com.apilytics.core.config.CacheConfig
import org.slf4j.LoggerFactory

import java.io._
import java.net.{HttpURLConnection, URL}
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.util.{Try, Using}

/** Caches parsed OpenAPI specs to avoid re-parsing on every catalog initialization.
  *
  * Cache key is SHA-256 hash of the spec location (URL or file path).
  * Invalidation uses:
  * - TTL if configured
  * - ETag/Last-Modified headers for remote specs
  * - File mtime for local specs
  */
object SpecCache {

  private val log = LoggerFactory.getLogger(getClass)

  private val CacheVersion = 1 // Bump when ParsedSpec format changes

  /** Cache entry with metadata for invalidation. */
  @SerialVersionUID(1L)
  private case class CacheEntry(
      spec: ParsedSpec,
      cachedAt: Long,
      etag: Option[String],
      lastModified: Option[Long],
      version: Int = CacheVersion
  ) extends Serializable

  /** Get a parsed spec, using cache if available and valid. */
  def getOrParse(specLocation: String, config: CacheConfig): ParsedSpec = {
    if (!config.enabled) {
      return Parser.parse(specLocation)
    }

    val cacheDir = getCacheDir(config)
    val cacheFile = cacheDir.resolve(cacheKey(specLocation))

    // Try to load from cache
    loadFromCache(cacheFile, specLocation, config) match {
      case Some(entry) if isValid(entry, specLocation, config) =>
        log.info("Using cached OpenAPI spec for {}", specLocation)
        entry.spec
      case _ =>
        log.info("Parsing OpenAPI spec: {}", specLocation)
        val spec = Parser.parse(specLocation)
        saveToCache(cacheFile, spec, specLocation)
        spec
    }
  }

  /** Clear all cached specs. */
  def clear(config: CacheConfig): Unit = {
    val cacheDir = getCacheDir(config)
    if (Files.exists(cacheDir)) {
      Files.list(cacheDir).forEach { path =>
        Try(Files.delete(path))
      }
      log.info("Cleared spec cache at {}", cacheDir)
    }
  }

  private def getCacheDir(config: CacheConfig): Path = {
    val dir = config.directory
      .map(Paths.get(_))
      .getOrElse(Paths.get(System.getProperty("user.home"), ".apilytics", "cache"))
    Files.createDirectories(dir)
    dir
  }

  private def cacheKey(specLocation: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hash = digest.digest(specLocation.getBytes("UTF-8"))
    hash.map("%02x".format(_)).mkString + ".cache"
  }

  private def loadFromCache(cacheFile: Path, specLocation: String, config: CacheConfig): Option[CacheEntry] = {
    if (!Files.exists(cacheFile)) return None

    Try {
      Using.resource(new ObjectInputStream(new FileInputStream(cacheFile.toFile))) { ois =>
        ois.readObject().asInstanceOf[CacheEntry]
      }
    }.toOption.filter(_.version == CacheVersion)
  }

  private def isValid(entry: CacheEntry, specLocation: String, config: CacheConfig): Boolean = {
    // Check TTL first
    config.ttl.foreach { ttl =>
      val expiresAt = entry.cachedAt + ttl.toMillis
      if (System.currentTimeMillis() > expiresAt) {
        log.debug("Cache entry expired (TTL)")
        return false
      }
    }

    // For remote URLs, check ETag/Last-Modified
    if (specLocation.startsWith("http://") || specLocation.startsWith("https://")) {
      checkRemoteValidity(specLocation, entry)
    } else {
      // For local files, check mtime
      checkLocalValidity(specLocation, entry)
    }
  }

  private def checkRemoteValidity(url: String, entry: CacheEntry): Boolean = {
    Try {
      val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("HEAD")
      conn.setConnectTimeout(5000)
      conn.setReadTimeout(5000)

      // Use conditional request if we have ETag
      entry.etag.foreach(e => conn.setRequestProperty("If-None-Match", e))

      try {
        conn.connect()
        if (conn.getResponseCode == 304) {
          // Not modified
          true
        } else if (conn.getResponseCode == 200) {
          // Check if ETag changed
          val serverEtag = Option(conn.getHeaderField("ETag"))
          val serverLastMod = Option(conn.getLastModified).filter(_ > 0)

          (entry.etag, serverEtag) match {
            case (Some(cached), Some(server)) => cached == server
            case _ =>
              (entry.lastModified, serverLastMod) match {
                case (Some(cached), Some(server)) => cached >= server
                case _ => false // No way to validate, re-fetch
              }
          }
        } else {
          false
        }
      } finally {
        conn.disconnect()
      }
    }.getOrElse {
      log.debug("Failed to check remote validity, treating cache as invalid")
      false
    }
  }

  private def checkLocalValidity(path: String, entry: CacheEntry): Boolean = {
    Try {
      val filePath = Paths.get(path)
      if (!Files.exists(filePath)) return false
      val mtime = Files.getLastModifiedTime(filePath).toMillis
      entry.lastModified.exists(_ >= mtime)
    }.getOrElse(false)
  }

  private def saveToCache(cacheFile: Path, spec: ParsedSpec, specLocation: String): Unit = {
    Try {
      val (etag, lastModified) = if (specLocation.startsWith("http://") || specLocation.startsWith("https://")) {
        fetchRemoteMetadata(specLocation)
      } else {
        (None, Try(Files.getLastModifiedTime(Paths.get(specLocation)).toMillis).toOption)
      }

      val entry = CacheEntry(
        spec = spec,
        cachedAt = System.currentTimeMillis(),
        etag = etag,
        lastModified = lastModified
      )

      Using.resource(new ObjectOutputStream(new FileOutputStream(cacheFile.toFile))) { oos =>
        oos.writeObject(entry)
      }
      log.debug("Cached OpenAPI spec to {}", cacheFile)
    }.recover { case e =>
      log.warn("Failed to cache OpenAPI spec: {}", e.getMessage)
    }
  }

  private def fetchRemoteMetadata(url: String): (Option[String], Option[Long]) = {
    Try {
      val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("HEAD")
      conn.setConnectTimeout(5000)
      conn.setReadTimeout(5000)
      try {
        conn.connect()
        val etag = Option(conn.getHeaderField("ETag"))
        val lastMod = Option(conn.getLastModified).filter(_ > 0)
        (etag, lastMod)
      } finally {
        conn.disconnect()
      }
    }.getOrElse((None, None))
  }
}

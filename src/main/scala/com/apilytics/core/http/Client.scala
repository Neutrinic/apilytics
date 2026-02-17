package com.apilytics.core.http

import cats.effect.{IO, Resource}
import com.apilytics.core.config.{AuthConfig, HttpConfig, ResponseFormat}
import fs2.Stream
import io.circe.Json
import org.http4s.{Header, MediaType, Request, Uri}
import org.http4s.circe._
import org.http4s.client.{Client => Http4sClient}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.Accept
import org.http4s.Status

import org.typelevel.ci.CIString

import java.time.Instant
import scala.concurrent.duration._

final case class ApiResponse(
    json: Json,
    status: Int,
    headers: Map[String, String]
)

object Client {

  /** Create a client resource with an externally-managed response cache.
    * The cache should be created at catalog initialization and passed here
    * so it persists across queries.
    */
  def resource(
      httpConfig: HttpConfig,
      authConfig: AuthConfig,
      responseCache: ResponseCache = ResponseCache.disabled
  ): Resource[IO, RestClient] = {
    // Defensive null check - responseCache may be null if config deserialization failed
    val cache = if (responseCache == null) ResponseCache.disabled else responseCache
    for {
      httpClient <- EmberClientBuilder.default[IO].withTimeout(httpConfig.timeout).build
      rateLimiter <- Resource.eval(
        httpConfig.rateLimit match {
          case Some(rps) => RateLimiter(rps)
          case None      => IO.pure(RateLimiter.unlimited)
        }
      )
    } yield new RestClient(httpClient, httpConfig, authConfig, None, rateLimiter, cache)
  }

  /** Create a client with OAuth2 token manager for dynamic token refresh. */
  def resourceWithOAuth2(
      httpConfig: HttpConfig,
      authConfig: AuthConfig,
      responseCache: ResponseCache = ResponseCache.disabled
  ): Resource[IO, RestClient] = {
    // Defensive null check - responseCache may be null if config deserialization failed
    val cache = if (responseCache == null) ResponseCache.disabled else responseCache
    val clientId = authConfig.clientId.getOrElse(
      throw new IllegalArgumentException("OAuth2 client credentials requires client-id")
    )
    val clientSecret = authConfig.clientSecret.getOrElse(
      throw new IllegalArgumentException("OAuth2 client credentials requires client-secret")
    )
    val tokenUrl = authConfig.tokenUrl.getOrElse(
      throw new IllegalArgumentException("OAuth2 client credentials requires token-url")
    )

    for {
      httpClient <- EmberClientBuilder.default[IO].withTimeout(httpConfig.timeout).build
      tokenManager <- Resource.eval(OAuth2TokenManager(clientId, clientSecret, tokenUrl, httpClient))
      rateLimiter <- Resource.eval(
        httpConfig.rateLimit match {
          case Some(rps) => RateLimiter(rps)
          case None      => IO.pure(RateLimiter.unlimited)
        }
      )
    } yield new RestClient(httpClient, httpConfig, authConfig, Some(tokenManager), rateLimiter, cache)
  }

  class RestClient(
      underlying: Http4sClient[IO],
      httpConfig: HttpConfig,
      authConfig: AuthConfig,
      tokenManager: Option[OAuth2TokenManager],
      rateLimiter: RateLimiter,
      responseCache: ResponseCache
  ) {
    private val applyAuth: Request[IO] => IO[Request[IO]] = tokenManager match {
      case Some(tm) => Auth.withTokenManager(tm)
      case None     => Auth(authConfig)
    }

    def get(uri: Uri, params: Map[String, String] = Map.empty): IO[ApiResponse] = {
      val fullUri = params.foldLeft(uri) { case (u, (k, v)) =>
        u.withQueryParam(k, v)
      }
      val baseReq = Request[IO](uri = fullUri)
      val endpoint = uri.path.renderString

      // Check cache first
      responseCache.get(endpoint, params).flatMap {
        case Some(cached) =>
          IO.pure(cached)
        case None =>
          // Apply rate limiting before making the request
          rateLimiter.acquire *>
            applyAuth(baseReq).flatMap { req =>
              executeWithRetry(req, baseReq, endpoint, params, attempt = 0, authRetried = false)
            }.flatTap { response =>
              // Only cache successful responses
              if (response.status >= 200 && response.status < 300) {
                responseCache.put(endpoint, params, response)
              } else IO.unit
            }
      }
    }

    /** Get cache statistics (hits, misses, size). */
    def cacheStats: IO[ResponseCache.Stats] = responseCache.stats

    /** Clear the response cache. */
    def clearCache: IO[Unit] = responseCache.clear()

    /** Stream response body with format-specific parsing.
      *
      * Unlike `get`, this streams records incrementally without buffering the
      * entire response. Use for streaming formats (NDJSON, SSE, MessagePack).
      *
      * Note: Streaming responses bypass caching and pagination since they
      * represent continuous data streams. Retries are attempted on connection
      * errors before streaming begins.
      */
    def getStreaming(
        uri: Uri,
        params: Map[String, String] = Map.empty,
        format: ResponseFormat
    ): Stream[IO, Json] = {
      val fullUri = params.foldLeft(uri) { case (u, (k, v)) =>
        u.withQueryParam(k, v)
      }

      // Set appropriate Accept header for the format
      val acceptHeader = format match {
        case ResponseFormat.Json        => Accept(MediaType.application.json)
        case ResponseFormat.NDJSON      => Accept(new MediaType("application", "x-ndjson"))
        case ResponseFormat.SSE         => Accept(new MediaType("text", "event-stream"))
        case ResponseFormat.MessagePack => Accept(new MediaType("application", "msgpack"))
      }

      val baseReq = Request[IO](uri = fullUri).putHeaders(acceptHeader)

      format match {
        case ResponseFormat.Json =>
          // Full-body JSON - wrap in stream
          Stream.eval(get(uri, params)).map(_.json)

        case ResponseFormat.NDJSON =>
          streamBodyWithRetry(baseReq, format).through(StreamingParsers.ndjson)

        case ResponseFormat.SSE =>
          streamBodyWithRetry(baseReq, format).through(StreamingParsers.sse)

        case ResponseFormat.MessagePack =>
          streamBodyWithRetry(baseReq, format).through(StreamingParsers.msgpack)
      }
    }

    /** Stream raw response body bytes with retry on connection errors.
      *
      * Retries are only attempted before streaming begins (on connection/initial response).
      * Once streaming starts, errors propagate immediately since we can't resume.
      */
    private def streamBodyWithRetry(
        baseReq: Request[IO],
        format: ResponseFormat,
        attempt: Int = 0
    ): Stream[IO, Byte] = {
      Stream.eval(rateLimiter.acquire) >>
        Stream.eval(applyAuth(baseReq)).flatMap { req =>
          Stream.resource(underlying.run(req)).flatMap { resp =>
            resp.status.code match {
              case code if code >= 200 && code < 300 =>
                resp.body

              case 429 if attempt < httpConfig.maxRetries =>
                // Rate limited - retry with backoff
                val retryAfter = resp.headers.get(CIString("Retry-After")).map { nel =>
                  val v = nel.head.value
                  v.toLongOption.map(_.seconds).getOrElse(exponentialBackoff(attempt))
                }
                val delay = retryAfter.getOrElse(exponentialBackoff(attempt))
                Stream.exec(resp.body.compile.drain *> IO.sleep(delay)) ++
                  streamBodyWithRetry(baseReq, format, attempt + 1)

              case code if code >= 500 && attempt < httpConfig.maxRetries =>
                // Server error - retry with backoff
                val delay = exponentialBackoff(attempt)
                Stream.exec(resp.body.compile.drain *> IO.sleep(delay)) ++
                  streamBodyWithRetry(baseReq, format, attempt + 1)

              case code =>
                // Non-retryable error
                Stream.eval(
                  resp.as[String].flatMap { body =>
                    IO.raiseError(ApiError.httpError(
                      endpoint = req.uri.path.renderString,
                      method = req.method,
                      params = Map.empty,
                      statusCode = code,
                      responseBody = body,
                      headers = resp.headers.headers.map(h => h.name.toString -> h.value).toMap,
                      retryAttempt = attempt
                    ))
                  }
                ).drain
            }
          }
        }.handleErrorWith {
          // Retry on network-level transient failures before streaming starts
          case e if isTransientNetworkError(e) && attempt < httpConfig.maxRetries =>
            val delay = exponentialBackoff(attempt)
            Stream.exec(IO.sleep(delay)) ++ streamBodyWithRetry(baseReq, format, attempt + 1)
          case e => Stream.raiseError[IO](e)
        }
    }

    private def executeWithRetry(
        req: Request[IO],
        baseReq: Request[IO],
        endpoint: String,
        params: Map[String, String],
        attempt: Int,
        authRetried: Boolean
    ): IO[ApiResponse] = {
      underlying.run(req).use { resp =>
        val hdrs = resp.headers.headers.map(h => h.name.toString -> h.value).toMap

        resp.status.code match {
          case code if code >= 200 && code < 300 =>
            resp.as[Json].map(json => ApiResponse(json, code, hdrs))

          case 429 if attempt < httpConfig.maxRetries =>
            val retryAfter = resp.headers.get(CIString("Retry-After")).map { nel =>
              val v = nel.head.value
              v.toLongOption.map(_.seconds).getOrElse {
                scala.util.Try {
                  val epoch = java.time.ZonedDateTime.parse(v, java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME).toEpochSecond
                  (epoch - Instant.now.getEpochSecond).seconds
                }.getOrElse(exponentialBackoff(attempt))
              }
            }
            val delay = retryAfter.getOrElse(exponentialBackoff(attempt))
            // Drain response body before retry
            resp.body.compile.drain *> IO.sleep(delay) *>
              executeWithRetry(req, baseReq, endpoint, params, attempt + 1, authRetried)

          case code if code >= 500 && attempt < httpConfig.maxRetries =>
            val delay = exponentialBackoff(attempt)
            resp.body.compile.drain *> IO.sleep(delay) *>
              executeWithRetry(req, baseReq, endpoint, params, attempt + 1, authRetried)

          case 401 if !authRetried && tokenManager.isDefined =>
            // Token may have expired - refresh once and retry
            resp.body.compile.drain *>
              tokenManager.get.refreshToken.flatMap { _ =>
                applyAuth(baseReq).flatMap { newReq =>
                  executeWithRetry(newReq, baseReq, endpoint, params, attempt, authRetried = true)
                }
              }

          case 401 | 403 =>
            resp.as[String].flatMap { body =>
              IO.raiseError(ApiError.authFailed(
                endpoint = endpoint,
                method = req.method,
                params = params,
                statusCode = resp.status.code,
                responseBody = body,
                headers = hdrs,
                retryAttempt = attempt
              ))
            }

          case code =>
            resp.as[String].flatMap { body =>
              IO.raiseError(ApiError.httpError(
                endpoint = endpoint,
                method = req.method,
                params = params,
                statusCode = code,
                responseBody = body,
                headers = hdrs,
                retryAttempt = attempt
              ))
            }
        }
      }.handleErrorWith {
        // Retry on network-level transient failures (connection timeout, socket errors, etc.)
        case e if isTransientNetworkError(e) && attempt < httpConfig.maxRetries =>
          val delay = exponentialBackoff(attempt)
          IO.sleep(delay) *>
            executeWithRetry(req, baseReq, endpoint, params, attempt + 1, authRetried)
        case e => IO.raiseError(e)
      }
    }

    /** Check if an exception is a transient network error worth retrying. */
    private def isTransientNetworkError(e: Throwable): Boolean = {
      import java.net.{ConnectException, SocketException, SocketTimeoutException}
      import java.io.IOException
      import java.util.concurrent.TimeoutException

      e match {
        case _: ConnectException       => true  // Connection refused, host unreachable
        case _: SocketException        => true  // Connection reset, broken pipe
        case _: SocketTimeoutException => true  // Read/connect timeout at socket level
        case _: TimeoutException       => true  // General timeout (e.g., from http4s)
        case e: IOException if e.getMessage != null &&
          (e.getMessage.contains("Connection reset") ||
           e.getMessage.contains("Broken pipe")) => true
        case _ => false
      }
    }

    private def exponentialBackoff(attempt: Int): FiniteDuration = {
      val base = math.pow(2, attempt).toLong
      val capped = math.min(base, httpConfig.maxBackoff.toSeconds)
      capped.seconds
    }
  }
}

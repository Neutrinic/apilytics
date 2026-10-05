package com.apilytics.core.http

import cats.effect.{IO, Ref, Resource}
import org.slf4j.LoggerFactory
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig, ResponseFormat}
import fs2.Stream
import io.circe.Json
import org.http4s.{Header, MediaType, Request, Uri}
import org.http4s.circe._
import org.http4s.client.{Client => Http4sClient}
import org.http4s.client.middleware.GZip
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
  /** Ember client with its own retrying switched off.
    *
    * Ember retries internally by default, beneath both our retry loop and the rate
    * limiter. That made `max-retries` untrue — attempts came out at 3 × (max-retries + 1)
    * — and, worse, let requests escape the rate limit entirely: `rateLimiter.acquire`
    * runs once per attempt we make, so a configured 60 requests per hour could issue 180
    * against a flaky connection. Retrying belongs in one place, where the limiter can see
    * it.
    */
  private def emberClient(httpConfig: HttpConfig): Resource[IO, org.http4s.client.Client[IO]] =
    EmberClientBuilder
      .default[IO]
      .withTimeout(httpConfig.timeout)
      .withRetryPolicy((_, _, _) => None)
      .build
      .map(prepared(_, httpConfig.timeout))

  /** Every HTTP client apilytics uses, the token manager's included: compressed, and bounded.
    *
    * `GZip` sends `Accept-Encoding: gzip, deflate` and decompresses a response that comes back
    * compressed, as a stream, so full-body JSON, NDJSON and SSE all read through it. Without it
    * nothing was decompressed: a server may compress when a request names no encoding, as
    * RFC 9110 allows and some gateways always do, and the read failed parsing gzip as JSON.
    * And an API that compresses only on request sent plain JSON, about ten times the bytes
    * (#347).
    */
  private[http] def prepared(
      client: org.http4s.client.Client[IO],
      timeout: FiniteDuration
  ): org.http4s.client.Client[IO] =
    GZip()(boundedAcquire(client, timeout))

  /** Bounds acquiring each response — connecting, sending, reading headers — by `timeout`.
    *
    * ember's own timeout does not cover opening the connection, so a request to a host
    * that drops packets waits for the operating system's TCP connect timeout: about two
    * minutes on Linux. On EMR Serverless, which has no internet access without a VPC, that
    * made each attempt take two minutes and a task with retries take 14 minutes to fail
    * (#266). A timeout here raises SocketTimeoutException, which the retry handler already
    * treats as transient, so retries and backoff are unchanged — only bounded. The response
    * body is not covered: it streams after acquisition, under ember's idle timeout.
    */
  private[http] def boundedAcquire(
      client: org.http4s.client.Client[IO],
      timeout: FiniteDuration
  ): org.http4s.client.Client[IO] =
    org.http4s.client.Client[IO] { req =>
      // Host and path only: user info and query parameters can carry credentials.
      val target = req.uri.authority.map(_.copy(userInfo = None).renderString).getOrElse("") +
        req.uri.path.renderString
      // Raced as Resources so the caller can still cancel a stalled acquisition, and a
      // response that arrives as the timeout fires is released rather than leaked.
      client.run(req).race(Resource.eval(IO.sleep(timeout))).flatMap {
        case Left(response) => Resource.pure[IO, org.http4s.Response[IO]](response)
        case Right(_) =>
          Resource.eval(IO.raiseError[org.http4s.Response[IO]](new java.net.SocketTimeoutException(
            s"No response from $target within $timeout: the host could not be reached, " +
              "or sent no response headers in time"
          )))
      }
    }

  /** The client every reader uses.
    *
    * `oauth2_client` with a `token-url` gets a token manager here, so tokens are fetched
    * and refreshed for whichever reader asks: the table scan, COUNT pushdown and aggregate
    * pushdown all build their client through this. Before #276 only `resourceWithOAuth2`
    * created one and no reader called it, so the documented config failed on the first
    * request asking for a pre-fetched token. A pre-fetched `token` with no `token-url`
    * is still sent as a static bearer token.
    */
  def resource(
      httpConfig: HttpConfig,
      authConfig: AuthConfig,
      responseCache: ResponseCache = ResponseCache.disabled
  ): Resource[IO, RestClient] = {
    // Defensive null check - responseCache may be null if config deserialization failed
    val cache = if (responseCache == null) ResponseCache.disabled else responseCache
    for {
      httpClient   <- emberClient(httpConfig)
      tokenManager <- oauth2TokenManager(authConfig, httpClient)
      rateLimiter <- Resource.eval(
        httpConfig.rateLimit match {
          case Some(rps) => RateLimiter(rps)
          case None      => IO.pure(RateLimiter.unlimited)
        }
      )
    } yield new RestClient(httpClient, httpConfig, authConfig, tokenManager, rateLimiter, cache)
  }

  /** A client that must use the OAuth2 token flow: fails unless `token-url` is set. */
  def resourceWithOAuth2(
      httpConfig: HttpConfig,
      authConfig: AuthConfig,
      responseCache: ResponseCache = ResponseCache.disabled
  ): Resource[IO, RestClient] =
    // Fails inside the Resource, like `resource`'s own checks, not when this is called.
    if (authConfig.tokenUrl.isEmpty)
      Resource.eval(IO.raiseError(new IllegalArgumentException("OAuth2 client credentials requires token-url")))
    else
      resource(httpConfig, authConfig.copy(authType = AuthType.OAuth2Client), responseCache)

  private def oauth2TokenManager(
      authConfig: AuthConfig,
      httpClient: Http4sClient[IO]
  ): Resource[IO, Option[OAuth2TokenManager]] =
    (authConfig.authType, authConfig.tokenUrl) match {
      case (AuthType.OAuth2Client, Some(tokenUrl)) =>
        val clientId = authConfig.clientId.getOrElse(
          throw new IllegalArgumentException("OAuth2 client credentials requires client-id")
        )
        val clientSecret = authConfig.clientSecret.getOrElse(
          throw new IllegalArgumentException("OAuth2 client credentials requires client-secret")
        )
        Resource.eval(OAuth2TokenManager(clientId, clientSecret, tokenUrl, httpClient)).map(Some(_))
      case _ => Resource.pure(None)
    }

  /** What one request attempt leads to (#313). */
  private sealed trait Attempt
  private object Attempt {
    final case class Done(response: ApiResponse)  extends Attempt
    final case class Retry(delay: FiniteDuration) extends Attempt
    case object RefreshAuth                       extends Attempt
    final case class Fail(error: Throwable)       extends Attempt
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

    /** Whether every request this client sends carries the source's credentials. Callers
      * that follow URLs an API hands back use it to keep those credentials on the API's
      * own origin (#288).
      */
    def sendsCredentials: Boolean = authConfig.authType != AuthType.None

    // Whose responses these are. The response cache is shared by every source in the JVM,
    // so a key of path and parameters alone let two catalogs requesting the same path, on
    // different hosts or with different credentials, be served each other's responses
    // (#278). The credentials are hashed, so no secret is held in a key or written to the
    // cache's debug log.
    private val credentialScope: String = {
      val a = authConfig
      val identity = List(
        a.authType.toString, a.token, a.username, a.password, a.headerName, a.headerValue,
        a.clientId, a.clientSecret, a.tokenUrl
      ).map(_.toString).mkString("\u0000")
      java.security.MessageDigest.getInstance("SHA-256")
        .digest(identity.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        .take(8).map("%02x".format(_)).mkString
    }

    // The URI's own query is part of the key too. Link-header pagination follows each next
    // link with its query inside the URL (`/items?page=2`) and an empty `params` map, so a
    // key of path and `params` alone gave every later page the same entry: page 3 was
    // served page 2 from the cache, and the walk followed the same link again (#286).
    private def cacheKey(uri: Uri): String = {
      val origin = uri.scheme.map(_.value + "://").getOrElse("") +
        uri.authority.map(_.copy(userInfo = None).renderString).getOrElse("")
      val query = if (uri.query.isEmpty) "" else "?" + uri.query.renderString
      s"$origin#$credentialScope${uri.path.renderString}$query"
    }

    def get(uri: Uri, params: Map[String, String] = Map.empty): IO[ApiResponse] = {
      val fullUri = params.foldLeft(uri) { case (u, (k, v)) =>
        u.withQueryParam(k, v)
      }
      val baseReq = Request[IO](uri = fullUri)
      val endpoint = uri.path.renderString
      // Keyed on the request actually sent: the URL with the parameters encoded into its
      // query, sorted so the same request always gets the same key. Composing the URL's own
      // query and the parameter map separately was ambiguous: `/items?a=1` with b=2, and
      // `/items?a=1?b=2` with no parameters, produced the same key (#286).
      val cached = cacheKey(params.toList.sorted.foldLeft(uri) { case (u, (k, v)) => u.withQueryParam(k, v) })

      // Check cache first
      responseCache.get(cached, Map.empty).flatMap {
        case Some(cached) =>
          IO.pure(cached)
        case None =>
          // executeWithRetry takes a rate-limit permit for each attempt it makes.
          applyAuth(baseReq).flatMap { req =>
              executeWithRetry(req, baseReq, endpoint, params, attempt = 0, authRetried = false)
            }.flatTap { response =>
              // Only cache successful responses
              if (response.status >= 200 && response.status < 300) {
                responseCache.put(cached, Map.empty, response)
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
      * entire response. Use for streaming formats (NDJSON, SSE).
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
        case ResponseFormat.Json   => Accept(MediaType.application.json)
        case ResponseFormat.NDJSON => Accept(new MediaType("application", "x-ndjson"))
        case ResponseFormat.SSE    => Accept(new MediaType("text", "event-stream"))
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
      }
    }

    /** Stream raw response body bytes with retry on connection errors.
      *
      * Retries stop the moment the first byte reaches the consumer. There is no resume
      * protocol here — a retry re-issues the request from the start — so retrying after
      * bytes have been emitted would replay records the consumer already has, and for
      * NDJSON or SSE would splice a half-written record onto a fresh response. `emitted`
      * records that transition, and once set a transient failure is raised rather than
      * retried: an error the caller can see beats silent duplication.
      *
      * Connection scoping: each retry attempt is scoped so the connection is released
      * before the backoff sleep. Only the successful streaming response keeps the
      * connection open for the duration of the stream.
      */
    private def streamBodyWithRetry(
        baseReq: Request[IO],
        format: ResponseFormat,
        attempt: Int = 0
    ): Stream[IO, Byte] =
      // As in executeWithRetry, the next attempt starts outside this attempt's error handler.
      // Appended inside it, a retry that hit a network error restarted the chain from this
      // attempt, sending more requests than max-retries allows (#313).
      Stream.eval(openStream(baseReq, attempt)).flatMap {
        case Left(delay) =>
          Stream.exec(IO.sleep(delay)) ++ streamBodyWithRetry(baseReq, format, attempt + 1)

        case Right((resp, release)) =>
          Stream.eval(Ref.of[IO, Boolean](false)).flatMap { emitted =>
            // Stream the body and release the connection when done. The flag flips on the
            // first chunk handed downstream, which is the point after which a retry would
            // duplicate rather than recover.
            resp.body.chunks
              .evalTap(_ => emitted.set(true))
              .flatMap(Stream.chunk)
              .onFinalize(release)
              .handleErrorWith {
                // Retry transient network failures, but only while nothing has been emitted.
                // Past that point the request cannot be replayed without duplicating records.
                case e if isTransientNetworkError(e) && attempt < httpConfig.maxRetries =>
                  Stream.eval(emitted.get).flatMap {
                    case false =>
                      Stream.exec(IO.sleep(exponentialBackoff(attempt))) ++
                        streamBodyWithRetry(baseReq, format, attempt + 1)
                    case true =>
                      Stream.exec(IO(
                        LoggerFactory.getLogger(getClass).warn(
                          "Connection failed after records were already delivered; not retrying, " +
                            "because re-issuing the request would replay them."
                        )
                      )) ++ Stream.raiseError[IO](e)
                  }
                case e => Stream.raiseError[IO](e)
              }
          }
      }

    /** One streaming request: the open response and its release, or how long to wait before
      * the next attempt. Each retry path releases its connection before returning, so no
      * connection is held through the backoff.
      */
    private def openStream(
        baseReq: Request[IO],
        attempt: Int
    ): IO[Either[FiniteDuration, (org.http4s.Response[IO], IO[Unit])]] =
      (rateLimiter.acquire *> applyAuth(baseReq)).flatMap { req =>
        underlying.run(req).allocated.flatMap { case (resp, release) =>
          resp.status.code match {
            case code if code >= 200 && code < 300 =>
              IO.pure(Right((resp, release)))

            case 429 if attempt < httpConfig.maxRetries =>
              // Rate limited - release connection, then wait
              resp.body.compile.drain.guarantee(release)
                .as(Left(retryAfterDelay(resp, attempt).getOrElse(exponentialBackoff(attempt))))

            case code if code >= 500 && attempt < httpConfig.maxRetries =>
              // Server error - release connection, then wait
              resp.body.compile.drain.guarantee(release).as(Left(exponentialBackoff(attempt)))

            case code =>
              // Non-retryable error - release connection and raise error
              readErrorBody(resp).guarantee(release).flatMap { body =>
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
          }
        }
      }.handleErrorWith {
        case e if isTransientNetworkError(e) && attempt < httpConfig.maxRetries =>
          IO.pure(Left(exponentialBackoff(attempt)))
        case e => IO.raiseError(e)
      }

    private def executeWithRetry(
        req: Request[IO],
        baseReq: Request[IO],
        endpoint: String,
        params: Map[String, String],
        attempt: Int,
        authRetried: Boolean
    ): IO[ApiResponse] =
      // One attempt decides what happens next; the sleep and the next attempt run after its
      // connection is released. The 5xx and 429 retries used to recurse inside the attempt's
      // `.use`, under the network-error handler of the attempt before: a retry that then hit
      // a network error was caught there and restarted the chain, so max-retries 5 could send
      // 11 requests (#313). The connection was also held through each backoff.
      attemptOnce(req, endpoint, params, attempt, authRetried).flatMap {
        case Attempt.Done(response) => IO.pure(response)
        case Attempt.Retry(delay) =>
          IO.sleep(delay) *> executeWithRetry(req, baseReq, endpoint, params, attempt + 1, authRetried)
        case Attempt.RefreshAuth =>
          // Token may have expired - refresh once and retry
          tokenManager.get.refreshToken *> applyAuth(baseReq).flatMap { newReq =>
            executeWithRetry(newReq, baseReq, endpoint, params, attempt, authRetried = true)
          }
        case Attempt.Fail(error) => IO.raiseError(error)
      }

    /** How long a 429's `Retry-After` asks us to wait: a number of seconds or an HTTP date.
      * A date already past means no wait; a value that is neither, or too large to be a
      * duration, falls back to backoff. Shared by full-body and streamed requests, which used
      * to read it differently: streams ignored dates and retried early, spending attempts and
      * permits on more 429s.
      *
      * The server's wait is honoured even beyond `max-backoff`, which bounds only our own
      * backoff: retrying sooner would only draw another 429.
      */
    private def retryAfterDelay(resp: org.http4s.Response[IO], attempt: Int): Option[FiniteDuration] =
      resp.headers.get(CIString("Retry-After")).map { nel =>
        val v = nel.head.value.trim
        v.toLongOption.flatMap(s => scala.util.Try(math.max(s, 0L).seconds).toOption).getOrElse {
          scala.util.Try {
            val epoch = java.time.ZonedDateTime.parse(v, java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME).toEpochSecond
            math.max(epoch - Instant.now.getEpochSecond, 0L).seconds
          }.getOrElse(exponentialBackoff(attempt))
        }
      }

    /** One request, and what to do about its result. */
    private def attemptOnce(
        req: Request[IO],
        endpoint: String,
        params: Map[String, String],
        attempt: Int,
        authRetried: Boolean
    ): IO[Attempt] = {
      // A permit per attempt, not per call: retries after 429, 5xx or a network error, and
      // the re-issue after an OAuth2 refresh, are requests too. Taking one only before the
      // first attempt let a burst of failures be retried above `rate-limit` (#279).
      rateLimiter.acquire *> underlying.run(req).use { resp =>
        val hdrs = resp.headers.headers.map(h => h.name.toString -> h.value).toMap

        resp.status.code match {
          case code if code >= 200 && code < 300 =>
            resp.as[Json].map(json => Attempt.Done(ApiResponse(json, code, hdrs)))

          case 429 if attempt < httpConfig.maxRetries =>
            // Drain response body before retry
            resp.body.compile.drain
              .as(Attempt.Retry(retryAfterDelay(resp, attempt).getOrElse(exponentialBackoff(attempt))))

          case code if code >= 500 && attempt < httpConfig.maxRetries =>
            resp.body.compile.drain.as(Attempt.Retry(exponentialBackoff(attempt)))

          case 401 if !authRetried && tokenManager.isDefined =>
            resp.body.compile.drain.as(Attempt.RefreshAuth)

          case 401 | 403 =>
            readErrorBody(resp).map { body =>
              Attempt.Fail(ApiError.authFailed(
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
            readErrorBody(resp).map { body =>
              Attempt.Fail(ApiError.httpError(
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
          IO.pure(Attempt.Retry(exponentialBackoff(attempt)))
        case e => IO.raiseError(e)
      }
    }

    /** Read at most `maxBytes` from an error response body to prevent OOM.
      * A malicious or misconfigured server could return a multi-GB error page;
      * we cap it at 4 KB which is plenty for diagnostic messages.
      */
    private val MaxErrorBodyBytes: Long = 4096

    private def readErrorBody(resp: org.http4s.Response[IO]): IO[String] =
      resp.body.take(MaxErrorBodyBytes).through(fs2.text.utf8.decode).compile.string

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

package com.apilytics.core.http

import cats.effect.{IO, Resource}
import com.apilytics.core.config.{AuthConfig, HttpConfig}
import io.circe.Json
import org.http4s.{Request, Uri}
import org.http4s.circe._
import org.http4s.client.{Client => Http4sClient}
import org.http4s.ember.client.EmberClientBuilder
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

  def resource(httpConfig: HttpConfig, authConfig: AuthConfig): Resource[IO, RestClient] = {
    for {
      httpClient <- EmberClientBuilder.default[IO].withTimeout(httpConfig.timeout).build
      rateLimiter <- Resource.eval(
        httpConfig.rateLimit match {
          case Some(rps) => RateLimiter(rps)
          case None      => IO.pure(RateLimiter.unlimited)
        }
      )
    } yield new RestClient(httpClient, httpConfig, authConfig, None, rateLimiter)
  }

  /** Create a client with OAuth2 token manager for dynamic token refresh. */
  def resourceWithOAuth2(
      httpConfig: HttpConfig,
      authConfig: AuthConfig
  ): Resource[IO, RestClient] = {
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
    } yield new RestClient(httpClient, httpConfig, authConfig, Some(tokenManager), rateLimiter)
  }

  class RestClient(
      underlying: Http4sClient[IO],
      httpConfig: HttpConfig,
      authConfig: AuthConfig,
      tokenManager: Option[OAuth2TokenManager],
      rateLimiter: RateLimiter
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

      // Apply rate limiting before making the request
      rateLimiter.acquire *>
        applyAuth(baseReq).flatMap { req =>
          executeWithRetry(req, baseReq, endpoint, params, attempt = 0, authRetried = false)
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
      }
    }

    private def exponentialBackoff(attempt: Int): FiniteDuration = {
      val base = math.pow(2, attempt).toLong
      val capped = math.min(base, httpConfig.maxBackoff.toSeconds)
      capped.seconds
    }
  }
}

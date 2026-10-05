package com.apilytics.core.http

import cats.effect.{IO, Ref, Resource}
import cats.effect.std.Semaphore
import io.circe.Json
import org.http4s.{Method, Request, Uri, UrlForm}
import org.http4s.circe._
import org.http4s.client.{Client => Http4sClient}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.`Content-Type`
import org.http4s.MediaType

import java.time.Instant
import scala.concurrent.duration._

/** Manages OAuth2 client credentials token lifecycle: fetch, cache, and refresh. */
class OAuth2TokenManager private (
    clientId: String,
    clientSecret: String,
    tokenUrl: Uri,
    httpClient: Http4sClient[IO],
    tokenRef: Ref[IO, Option[OAuth2TokenManager.CachedToken]],
    refreshLock: Semaphore[IO]
) {

  import OAuth2TokenManager._

  /** Get a valid access token, fetching or refreshing as needed. */
  def getToken: IO[String] = {
    tokenRef.get.flatMap {
      case Some(cached) if !cached.isExpired =>
        IO.pure(cached.accessToken)
      case _ =>
        refreshToken
    }
  }

  /** Force refresh the token. Used on 401 response.
    * Invalidates current token and fetches a new one.
    */
  def refreshToken: IO[String] = {
    // Use semaphore to prevent concurrent refresh attempts
    refreshLock.permit.use { _ =>
      // Always invalidate and fetch on forced refresh (401 means server rejected the token)
      tokenRef.set(None) *> fetchNewToken
    }
  }

  private def fetchNewToken: IO[String] = {
    val formData = UrlForm(
      "grant_type" -> "client_credentials",
      "client_id" -> clientId,
      "client_secret" -> clientSecret
    )

    val req = Request[IO](Method.POST, tokenUrl)
      .withEntity(formData)
      .withContentType(`Content-Type`(MediaType.application.`x-www-form-urlencoded`))

    val endpoint = tokenUrl.path.renderString

    httpClient.run(req).use { resp =>
      val hdrs = resp.headers.headers.map(h => h.name.toString -> h.value).toMap

      resp.status.code match {
        case code if code >= 200 && code < 300 =>
          resp.as[Json].flatMap { json =>
            val cursor = json.hcursor
            cursor.get[String]("access_token") match {
              case Right(accessToken) =>
                val expiresIn = cursor.get[Long]("expires_in").getOrElse(3600L)
                // Expire 60s early to avoid edge cases
                val expiresAt = Instant.now.plusSeconds(expiresIn - 60)
                val cached = CachedToken(accessToken, expiresAt)
                tokenRef.set(Some(cached)).as(accessToken)

              case Left(_) =>
                // A successful token response is made of credentials, under whatever name
                // the IdP chose (`accessToken`, say), so no value of it goes into the error,
                // which reaches logs. The field names are enough to see what was sent (#315).
                val fields = json.asObject.map(_.keys.mkString(", ")).getOrElse("none: not a JSON object")
                IO.raiseError(ApiError(
                  message = "OAuth2 response missing access_token field",
                  endpoint = endpoint,
                  method = Method.POST,
                  params = Map.empty,
                  statusCode = code,
                  responseBody = s"(values withheld) fields: $fields",
                  requestId = ApiError.extractRequestId(hdrs),
                  retryAttempt = 0
                ))
            }
          }
        case code =>
          resp.as[String].flatMap { body =>
            IO.raiseError(ApiError.authFailed(
              endpoint = endpoint,
              method = Method.POST,
              params = Map.empty,
              statusCode = code,
              responseBody = body,
              headers = hdrs,
              retryAttempt = 0
            ))
          }
      }
    }
  }
}

object OAuth2TokenManager {

  final case class CachedToken(accessToken: String, expiresAt: Instant) {
    def isExpired: Boolean = Instant.now.isAfter(expiresAt)
  }

  /** Create a token manager resource. The HTTP client is managed internally. */
  def resource(
      clientId: String,
      clientSecret: String,
      tokenUrl: String,
      timeout: FiniteDuration = 30.seconds,
      compression: Boolean = true
  ): Resource[IO, OAuth2TokenManager] = {
    val uri = Uri.unsafeFromString(tokenUrl)

    for {
      // Bounded like the data client: ember's timeout does not cover connecting (#266).
      client <- EmberClientBuilder.default[IO].withTimeout(timeout).build.map(Client.prepared(_, timeout, compression))
      tokenRef <- Resource.eval(Ref.of[IO, Option[CachedToken]](None))
      lock <- Resource.eval(Semaphore[IO](1))
    } yield new OAuth2TokenManager(clientId, clientSecret, uri, client, tokenRef, lock)
  }

  /** Create a token manager with a pre-existing HTTP client. */
  def apply(
      clientId: String,
      clientSecret: String,
      tokenUrl: String,
      httpClient: Http4sClient[IO]
  ): IO[OAuth2TokenManager] = {
    val uri = Uri.unsafeFromString(tokenUrl)
    for {
      tokenRef <- Ref.of[IO, Option[CachedToken]](None)
      lock <- Semaphore[IO](1)
    } yield new OAuth2TokenManager(clientId, clientSecret, uri, httpClient, tokenRef, lock)
  }
}

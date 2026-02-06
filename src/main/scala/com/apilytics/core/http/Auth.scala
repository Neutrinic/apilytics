package com.apilytics.core.http

import cats.effect.IO
import com.apilytics.core.config.{AuthConfig, AuthType}
import org.http4s.{Header, Request}
import org.http4s.headers.Authorization
import org.http4s.Credentials
import org.typelevel.ci.CIString

object Auth {

  /** Apply auth to a request based on config. For OAuth2, use withTokenManager instead. */
  def apply(config: AuthConfig): Request[IO] => IO[Request[IO]] = config.authType match {
    case AuthType.None =>
      req => IO.pure(req)

    case AuthType.Bearer =>
      val token = config.token.getOrElse(throw new IllegalArgumentException("Bearer auth requires token"))
      req => IO.pure(req.putHeaders(Authorization(Credentials.Token(CIString("Bearer"), token))))

    case AuthType.Basic =>
      val user = config.username.getOrElse(throw new IllegalArgumentException("Basic auth requires username"))
      val pass = config.password.getOrElse(throw new IllegalArgumentException("Basic auth requires password"))
      val encoded = java.util.Base64.getEncoder.encodeToString(s"$user:$pass".getBytes("UTF-8"))
      req => IO.pure(req.putHeaders(Authorization(Credentials.Token(CIString("Basic"), encoded))))

    case AuthType.Header =>
      val name = config.headerName.getOrElse(throw new IllegalArgumentException("Header auth requires header-name"))
      val value = config.headerValue.getOrElse(throw new IllegalArgumentException("Header auth requires header-value"))
      req => IO.pure(req.putHeaders(Header.Raw(CIString(name), value)))

    case AuthType.OAuth2Client =>
      // Static token fallback (for pre-fetched tokens)
      val token = config.token.getOrElse(
        throw new IllegalArgumentException("OAuth2 client credentials requires token-url or pre-fetched token")
      )
      req => IO.pure(req.putHeaders(Authorization(Credentials.Token(CIString("Bearer"), token))))
  }

  /** Apply OAuth2 auth using a token manager for dynamic token refresh. */
  def withTokenManager(tokenManager: OAuth2TokenManager): Request[IO] => IO[Request[IO]] = { req =>
    tokenManager.getToken.map { token =>
      req.putHeaders(Authorization(Credentials.Token(CIString("Bearer"), token)))
    }
  }
}

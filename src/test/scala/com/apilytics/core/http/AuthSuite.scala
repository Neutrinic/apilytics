package com.apilytics.core.http

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType}
import munit.FunSuite
import org.http4s.Request

class AuthSuite extends FunSuite {

  private def applyAuth(config: AuthConfig): Request[IO] = {
    val f = Auth(config)
    f(Request[IO]()).unsafeRunSync()
  }

  test("bearer auth adds Authorization header") {
    val config = AuthConfig(authType = AuthType.Bearer, token = Some("mytoken"))
    val req = applyAuth(config)
    val header = req.headers.get(org.typelevel.ci.CIString("Authorization")).get.head.value
    assert(header.contains("Bearer"))
    assert(header.contains("mytoken"))
  }

  test("basic auth adds base64 encoded header") {
    val config = AuthConfig(authType = AuthType.Basic, username = Some("user"), password = Some("pass"))
    val req = applyAuth(config)
    val header = req.headers.get(org.typelevel.ci.CIString("Authorization")).get.head.value
    assert(header.contains("Basic"))
    val encoded = java.util.Base64.getEncoder.encodeToString("user:pass".getBytes("UTF-8"))
    assert(header.contains(encoded))
  }

  test("header auth adds custom header") {
    val config = AuthConfig(authType = AuthType.Header, headerName = Some("X-Api-Key"), headerValue = Some("secret"))
    val req = applyAuth(config)
    val header = req.headers.get(org.typelevel.ci.CIString("X-Api-Key")).get.head.value
    assertEquals(header, "secret")
  }

  test("bearer auth without token throws") {
    val config = AuthConfig(authType = AuthType.Bearer)
    intercept[IllegalArgumentException] {
      Auth(config)
    }
  }

  test("basic auth without username throws") {
    val config = AuthConfig(authType = AuthType.Basic, password = Some("pass"))
    intercept[IllegalArgumentException] {
      Auth(config)
    }
  }

  test("header auth without header-name throws") {
    val config = AuthConfig(authType = AuthType.Header, headerValue = Some("val"))
    intercept[IllegalArgumentException] {
      Auth(config)
    }
  }
}

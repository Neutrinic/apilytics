package com.apilytics.core.http

import munit.FunSuite
import org.http4s.Method

class ApiErrorSuite extends FunSuite {

  test("getMessage formats error with all context") {
    val error = ApiError(
      message = "Not Found",
      endpoint = "/api/v2/pokemon/9999",
      method = Method.GET,
      params = Map("limit" -> "10"),
      statusCode = 404,
      responseBody = """{"error": "Pokemon not found"}""",
      requestId = Some("req-123"),
      retryAttempt = 0
    )

    val msg = error.getMessage
    assert(msg.contains("Not Found"))
    assert(msg.contains("GET /api/v2/pokemon/9999?limit=10"))
    assert(msg.contains("Status: 404"))
    assert(msg.contains("Request-ID: req-123"))
    assert(msg.contains("""{"error": "Pokemon not found"}"""))
  }

  test("getMessage shows retry count when > 0") {
    val error = ApiError(
      message = "Service Unavailable",
      endpoint = "/api/data",
      method = Method.GET,
      params = Map.empty,
      statusCode = 503,
      responseBody = "Server overloaded",
      requestId = None,
      retryAttempt = 3
    )

    val msg = error.getMessage
    assert(msg.contains("after 3 retries"))
  }

  test("getMessage truncates long response body at 200 chars") {
    val longBody = "x" * 300
    val error = ApiError(
      message = "Bad Request",
      endpoint = "/api",
      method = Method.GET,
      params = Map.empty,
      statusCode = 400,
      responseBody = longBody,
      requestId = None,
      retryAttempt = 0
    )

    val msg = error.getMessage
    assert(msg.contains("..."))
    val bodyLine = msg.split("\n").find(_.contains("Response:")).get
    // "  Response: " (12 chars) + 200 truncated + "..." (3 chars) = 215
    assert(bodyLine.length <= 215, s"Body line too long: ${bodyLine.length}")
  }

  test("getMessage omits Request-ID when not present") {
    val error = ApiError(
      message = "Error",
      endpoint = "/api",
      method = Method.GET,
      params = Map.empty,
      statusCode = 500,
      responseBody = "error",
      requestId = None,
      retryAttempt = 0
    )

    val msg = error.getMessage
    assert(!msg.contains("Request-ID"))
  }

  test("getMessage omits params when empty") {
    val error = ApiError(
      message = "Error",
      endpoint = "/api/users",
      method = Method.GET,
      params = Map.empty,
      statusCode = 500,
      responseBody = "error",
      requestId = None,
      retryAttempt = 0
    )

    val msg = error.getMessage
    assert(msg.contains("GET /api/users"))
    assert(!msg.contains("?"))
  }

  test("extractRequestId finds common header names") {
    val headers1 = Map("X-Request-Id" -> "id1")
    assertEquals(ApiError.extractRequestId(headers1), Some("id1"))

    val headers2 = Map("x-correlation-id" -> "id2")
    assertEquals(ApiError.extractRequestId(headers2), Some("id2"))

    val headers3 = Map("Request-ID" -> "id3")
    assertEquals(ApiError.extractRequestId(headers3), Some("id3"))

    val headers4 = Map("Content-Type" -> "application/json")
    assertEquals(ApiError.extractRequestId(headers4), None)
  }

  test("httpError maps status codes to descriptive messages") {
    val error404 = ApiError.httpError("/api", Method.GET, Map.empty, 404, "", Map.empty, 0)
    assertEquals(error404.message, "Not Found")

    val error429 = ApiError.httpError("/api", Method.GET, Map.empty, 429, "", Map.empty, 0)
    assertEquals(error429.message, "Too Many Requests")

    val error503 = ApiError.httpError("/api", Method.GET, Map.empty, 503, "", Map.empty, 0)
    assertEquals(error503.message, "Service Unavailable")

    val error418 = ApiError.httpError("/api", Method.GET, Map.empty, 418, "", Map.empty, 0)
    assertEquals(error418.message, "Request Failed") // unknown status
  }

  test("authFailed creates error with auth message") {
    val error = ApiError.authFailed(
      endpoint = "/api/protected",
      method = Method.GET,
      params = Map.empty,
      statusCode = 401,
      responseBody = "Unauthorized",
      headers = Map("X-Request-Id" -> "auth-req-1"),
      retryAttempt = 0
    )

    assertEquals(error.message, "Authentication failed")
    assertEquals(error.statusCode, 401)
    assertEquals(error.requestId, Some("auth-req-1"))
  }

  test("redactParams redacts sensitive keys") {
    val params = Map(
      "api_key" -> "sk-12345",
      "limit" -> "10",
      "access_token" -> "ghp_abc123",
      "offset" -> "0"
    )
    val redacted = ApiError.redactParams(params)
    assertEquals(redacted("api_key"), "[REDACTED]")
    assertEquals(redacted("access_token"), "[REDACTED]")
    assertEquals(redacted("limit"), "10")
    assertEquals(redacted("offset"), "0")
  }

  test("redactParams is case-insensitive") {
    val params = Map(
      "API_KEY" -> "secret",
      "X-Auth-Token" -> "secret",
      "Password" -> "secret",
      "CLIENT_SECRET" -> "secret"
    )
    val redacted = ApiError.redactParams(params)
    assert(redacted.values.forall(_ == "[REDACTED]"))
  }

  test("redactParams leaves non-sensitive keys unchanged") {
    val params = Map("limit" -> "10", "offset" -> "0", "page" -> "1", "sort" -> "name")
    val redacted = ApiError.redactParams(params)
    assertEquals(redacted, params)
  }

  test("renderParams returns empty string for empty params") {
    assertEquals(ApiError.renderParams(Map.empty), "")
  }

  test("renderParams redacts sensitive values in query string") {
    val params = Map("api_key" -> "secret123", "limit" -> "10")
    val result = ApiError.renderParams(params)
    assert(result.contains("api_key=[REDACTED]"))
    assert(result.contains("limit=10"))
    assert(!result.contains("secret123"))
  }

  test("getMessage redacts sensitive params") {
    val error = ApiError(
      message = "Unauthorized",
      endpoint = "/api/data",
      method = Method.GET,
      params = Map("api_key" -> "sk-secret-key", "limit" -> "10"),
      statusCode = 401,
      responseBody = "Unauthorized",
      requestId = None,
      retryAttempt = 0
    )

    val msg = error.getMessage
    assert(msg.contains("api_key=[REDACTED]"))
    assert(msg.contains("limit=10"))
    assert(!msg.contains("sk-secret-key"))
  }

  test("original params remain accessible for programmatic use") {
    val error = ApiError(
      message = "Error",
      endpoint = "/api",
      method = Method.GET,
      params = Map("api_key" -> "sk-secret-key"),
      statusCode = 401,
      responseBody = "",
      requestId = None,
      retryAttempt = 0
    )

    // Case class field retains original value
    assertEquals(error.params("api_key"), "sk-secret-key")
    // But getMessage is redacted
    assert(!error.getMessage.contains("sk-secret-key"))
  }
}

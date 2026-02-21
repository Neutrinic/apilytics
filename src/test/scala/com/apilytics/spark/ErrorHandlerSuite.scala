package com.apilytics.spark

import munit.FunSuite
import com.apilytics.core.http.ApiError
import org.http4s.Method

class ErrorHandlerSuite extends FunSuite {

  test("ApiError is reformatted cleanly") {
    val apiError = ApiError(
      message = "Authentication failed",
      endpoint = "/v1/users",
      method = Method.GET,
      params = Map("limit" -> "10"),
      statusCode = 401,
      responseBody = """{"error": "invalid_token"}""",
      requestId = Some("req-123"),
      retryAttempt = 0
    )

    val ex = intercept[ApiException] {
      ErrorHandler.withCleanErrors {
        throw apiError
      }
    }

    assert(ex.getMessage.contains("Authentication failed (401)"))
    assert(ex.getMessage.contains("GET /v1/users"))
  }

  test("IllegalArgumentException becomes config error") {
    val ex = intercept[ApiException] {
      ErrorHandler.withCleanErrors {
        throw new IllegalArgumentException("Missing required field: openapi")
      }
    }

    assert(ex.getMessage.contains("Configuration error"))
    assert(ex.getMessage.contains("Missing required field"))
  }

  test("ApiException passes through unchanged") {
    val original = ApiException("Already clean")

    val ex = intercept[ApiException] {
      ErrorHandler.withCleanErrors {
        throw original
      }
    }

    assertEquals(ex, original)
  }

  test("toString does not include stack trace") {
    val ex = ApiException("Test error")
    assertEquals(ex.toString, "ApiException: Test error")
  }

  test("ApiError with sensitive params is redacted in clean error") {
    val apiError = ApiError(
      message = "Authentication failed",
      endpoint = "/v1/users",
      method = Method.GET,
      params = Map("api_key" -> "sk-secret", "limit" -> "10"),
      statusCode = 401,
      responseBody = """{"error": "invalid_token"}""",
      requestId = Some("req-123"),
      retryAttempt = 0
    )

    val ex = intercept[ApiException] {
      ErrorHandler.withCleanErrors {
        throw apiError
      }
    }

    assert(ex.getMessage.contains("api_key=[REDACTED]"))
    assert(ex.getMessage.contains("limit=10"))
    assert(!ex.getMessage.contains("sk-secret"))
  }
}

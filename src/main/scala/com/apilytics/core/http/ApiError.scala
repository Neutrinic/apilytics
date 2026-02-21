package com.apilytics.core.http

import org.http4s.{Method, Uri}

/**
 * Rich exception for API errors with full request context.
 *
 * @param message Human-readable error description
 * @param endpoint The API endpoint path (e.g., "/api/v2/pokemon")
 * @param method HTTP method used
 * @param params Query parameters sent with the request
 * @param statusCode HTTP status code returned
 * @param responseBody Response body (may be truncated for large responses)
 * @param requestId Request ID from response headers, if present
 * @param retryAttempt Which retry attempt failed (0 = first attempt)
 * @param cause Underlying exception, if any
 */
final case class ApiError(
    message: String,
    endpoint: String,
    method: Method,
    params: Map[String, String],
    statusCode: Int,
    responseBody: String,
    requestId: Option[String],
    retryAttempt: Int,
    cause: Option[Throwable] = None
) extends RuntimeException(message, cause.orNull) {

  override def getMessage: String = {
    val paramsStr = ApiError.renderParams(params)
    val requestIdStr = requestId.map(id => s"\n  Request-ID: $id").getOrElse("")
    val retryStr = if (retryAttempt > 0) s" (after $retryAttempt retries)" else ""
    val bodyPreview = if (responseBody.length > 200) responseBody.take(200) + "..." else responseBody

    s"""API Error: $message$retryStr
       |  ${method.name} $endpoint$paramsStr
       |  Status: $statusCode$requestIdStr
       |  Response: $bodyPreview""".stripMargin
  }
}

object ApiError {

  /** Common request ID header names to check. */
  private val requestIdHeaders = List(
    "X-Request-Id",
    "X-Request-ID",
    "Request-Id",
    "Request-ID",
    "X-Correlation-Id",
    "X-Correlation-ID",
    "Correlation-Id"
  )

  /** Extract request ID from response headers. */
  def extractRequestId(headers: Map[String, String]): Option[String] = {
    val lowerHeaders = headers.map { case (k, v) => k.toLowerCase -> v }
    requestIdHeaders.collectFirst {
      case h if lowerHeaders.contains(h.toLowerCase) => lowerHeaders(h.toLowerCase)
    }
  }

  /** Create an auth failure error. */
  def authFailed(
      endpoint: String,
      method: Method,
      params: Map[String, String],
      statusCode: Int,
      responseBody: String,
      headers: Map[String, String],
      retryAttempt: Int
  ): ApiError = ApiError(
    message = s"Authentication failed",
    endpoint = endpoint,
    method = method,
    params = params,
    statusCode = statusCode,
    responseBody = responseBody,
    requestId = extractRequestId(headers),
    retryAttempt = retryAttempt
  )

  /** Regex for sensitive parameter names. Matches common credential-bearing keys
    * such as api_key, access_token, password, client_secret, authorization, etc.
    */
  private val sensitiveKeyPattern = "(?i).*(key|token|pass|secret|auth|credential).*".r

  /** Redact values for params whose keys match sensitive patterns.
    * Returns a copy with sensitive values replaced by `[REDACTED]`.
    */
  def redactParams(params: Map[String, String]): Map[String, String] =
    params.map {
      case (k, _) if sensitiveKeyPattern.matches(k) => k -> "[REDACTED]"
      case kv => kv
    }

  /** Render params as a query string with sensitive values redacted.
    * Returns empty string if params is empty, otherwise `?k1=v1&k2=v2`.
    */
  def renderParams(params: Map[String, String]): String = {
    if (params.isEmpty) ""
    else {
      val safe = redactParams(params)
      "?" + safe.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("&")
    }
  }

  /** Create a generic HTTP error. */
  def httpError(
      endpoint: String,
      method: Method,
      params: Map[String, String],
      statusCode: Int,
      responseBody: String,
      headers: Map[String, String],
      retryAttempt: Int
  ): ApiError = {
    val statusMessage = statusCode match {
      case 400 => "Bad Request"
      case 401 => "Unauthorized"
      case 403 => "Forbidden"
      case 404 => "Not Found"
      case 405 => "Method Not Allowed"
      case 408 => "Request Timeout"
      case 409 => "Conflict"
      case 422 => "Unprocessable Entity"
      case 429 => "Too Many Requests"
      case 500 => "Internal Server Error"
      case 502 => "Bad Gateway"
      case 503 => "Service Unavailable"
      case 504 => "Gateway Timeout"
      case _   => "Request Failed"
    }

    ApiError(
      message = statusMessage,
      endpoint = endpoint,
      method = method,
      params = params,
      statusCode = statusCode,
      responseBody = responseBody,
      requestId = extractRequestId(headers),
      retryAttempt = retryAttempt
    )
  }
}

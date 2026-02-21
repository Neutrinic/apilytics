package com.apilytics.spark

import com.apilytics.core.http.ApiError
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.AnalysisException

import scala.util.control.NonFatal

/** Clean error formatting for Apilytics operations.
  *
  * Catches Spark's verbose exceptions and reformats them into concise, actionable messages.
  */
object ErrorHandler {

  /** Execute a block with clean error handling.
    *
    * Catches exceptions and reformats them. If reformatting isn't possible,
    * throws the original exception.
    */
  def withCleanErrors[T](block: => T): T = {
    try {
      block
    } catch {
      case e: ApiException =>
        // Already clean, re-throw
        throw e

      case e: ApiError =>
        // Our HTTP client error - already has good message
        throw ApiException(formatApiError(e), e)

      case e: NoSuchTableException =>
        throw ApiException(formatTableNotFound(e), e)

      case e: NoSuchNamespaceException =>
        throw ApiException(formatNamespaceNotFound(e), e)

      case e: AnalysisException =>
        throw ApiException(formatAnalysisException(e), e)

      case e: IllegalArgumentException if e.getMessage != null =>
        // Config errors often come as IllegalArgumentException
        throw ApiException(s"Configuration error: ${e.getMessage}", e)

      case NonFatal(e) =>
        // For other errors, try to extract a useful message
        val rootCause = findRootCause(e)
        val message = extractCleanMessage(rootCause)
        throw ApiException(message, e)
    }
  }

  /** Format API errors (401, 404, 500, etc.) */
  private def formatApiError(e: ApiError): String = {
    val status = e.statusCode match {
      case 401 => "Authentication failed (401)"
      case 403 => "Access denied (403)"
      case 404 => "Not found (404)"
      case 429 => "Rate limit exceeded (429)"
      case s if s >= 500 => s"Server error ($s)"
      case s => s"HTTP error ($s)"
    }
    val paramsStr = ApiError.renderParams(e.params)
    s"""$status
       |  ${e.method.name} ${e.endpoint}$paramsStr
       |  ${e.message}""".stripMargin.trim
  }

  /** Format table not found errors */
  private def formatTableNotFound(e: NoSuchTableException): String = {
    // Spark 4.0 uses: "[TABLE_OR_VIEW_NOT_FOUND] The table or view `catalog`.`ns`.`table` cannot be found"
    // Extract table name from the exception's tableName method if available, otherwise parse message
    val tableName = try {
      // NoSuchTableException has a tableName() method in Spark 4.0
      e.getClass.getMethod("tableName").invoke(e).toString
    } catch {
      case _: Exception =>
        // Fallback: try to extract from message
        val msg = Option(e.getMessage).getOrElse("")
        val backtickPattern = """`([^`]+)`""".r
        backtickPattern.findAllIn(msg).toList.lastOption.getOrElse("unknown")
    }
    s"Table not found: $tableName"
  }

  /** Format namespace not found errors */
  private def formatNamespaceNotFound(e: NoSuchNamespaceException): String = {
    s"Namespace not found: ${e.getMessage}"
  }

  /** Format Spark AnalysisException (column not found, syntax errors, etc.) */
  private def formatAnalysisException(e: AnalysisException): String = {
    val msg = e.getMessage
    if (msg == null) return "Query analysis failed"

    // Extract the first line which usually has the useful info
    val firstLine = msg.split("\n").head

    // Pattern: [ERROR_CODE] actual message
    val codePattern = """\[([A-Z_]+(?:\.[A-Z_]+)*)\]\s*(.+)""".r
    firstLine match {
      case codePattern(code, message) =>
        val friendlyCode = code match {
          case "UNRESOLVED_COLUMN.WITH_SUGGESTION" => "Column not found"
          case "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION" => "Column not found"
          case "TABLE_OR_VIEW_NOT_FOUND" => "Table not found"
          case "PARSE_SYNTAX_ERROR" => "SQL syntax error"
          case _ => code.replace("_", " ").toLowerCase.capitalize
        }
        s"$friendlyCode: $message"
      case _ =>
        // Just return the first line, trimmed
        firstLine.take(200)
    }
  }

  /** Find the root cause of an exception chain */
  private def findRootCause(e: Throwable): Throwable = {
    var current = e
    while (current.getCause != null && current.getCause != current) {
      current = current.getCause
    }
    current
  }

  /** Extract a clean message from an exception */
  private def extractCleanMessage(e: Throwable): String = {
    val msg = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
    // Truncate very long messages
    if (msg.length > 300) msg.take(300) + "..."
    else msg
  }
}

/** Clean exception thrown by Apilytics error handling.
  *
  * Contains a formatted message without stack traces when printed.
  * Does NOT include a cause or stack trace - this is intentional for clean REPL output.
  * The original exception is logged separately if debugging is needed.
  */
class ApiException private (message: String)
    extends RuntimeException(message, null, true, false) {
  // The (message, cause=null, enableSuppression=true, writableStackTrace=false) constructor
  // prevents the stack trace from being filled in, giving us cleaner output.
  // We intentionally don't pass the cause to avoid "Caused by" chains in REPL output.

  /** Override to NOT print stack trace by default. */
  override def toString: String = s"ApiException: $message"

  /** Print just the message, no stack trace. */
  def printMessage(): Unit = {
    System.err.println(s"\n❌ $message\n")
  }
}

object ApiException {
  /** Create an ApiException.
    *
    * @param message Clean, formatted error message
    * @param originalError The original exception for debug logging only (not set as cause)
    */
  def apply(message: String, originalError: Throwable = null): ApiException = {
    // Log the original error for debugging if needed
    if (originalError != null && sys.props.get("apilytics.debug").contains("true")) {
      System.err.println(s"[DEBUG] Original exception: ${originalError.getClass.getName}: ${originalError.getMessage}")
    }
    new ApiException(message)
  }
}

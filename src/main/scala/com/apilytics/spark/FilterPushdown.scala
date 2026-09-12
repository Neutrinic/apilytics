package com.apilytics.spark

import com.apilytics.core.config.FilterConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.{Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate

/** Shared filter and limit pushdown logic for scan builders.
  * Both RESTScanBuilder and ExplodedArrayScanBuilder mix in this trait
  * to map Spark SQL predicates to API query parameters.
  */
trait FilterPushdown extends Logging {

  protected def filterConfigs: List[FilterConfig]

  protected var pushedParams: Map[String, String] = Map.empty
  protected var pushedLimit: Option[Int] = None
  protected var _pushedPredicates: Array[Predicate] = Array.empty

  def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val results = predicates.map(p => p -> matchPredicate(p))

    // A pushed predicate is a promise: Spark removes it from the plan and never re-checks
    // it. A request carries one value per query parameter, so when two predicates resolve
    // to the same parameter only one can actually be sent — `created_at >= X AND
    // created_at <= Y` against a single `since` parameter being the ordinary case. Keeping
    // the first and claiming both would drop a filter nobody applies, and the query would
    // return rows outside the range rather than merely reading too many.
    //
    // The first claimant wins and the rest stay local. Declining all of them would be safe
    // too, but needlessly gives up the narrowing the first one buys.
    val claimed = scala.collection.mutable.Set.empty[String]
    val pushed  = Array.newBuilder[(Predicate, String, String)]
    val locals  = Array.newBuilder[Predicate]

    results.foreach {
      case (p, Some((param, value))) if claimed.add(param) => pushed += ((p, param, value))
      case (p, Some((param, _))) =>
        logInfo(
          s"Filter ${formatPredicate(p)} matches parameter '$param', already carrying " +
            "another predicate's value; Spark will apply this one."
        )
        locals += p
      case (p, None) => locals += p
    }

    val pushedTriples = pushed.result()
    _pushedPredicates = pushedTriples.map(_._1)
    pushedParams = pushedTriples.map(t => t._2 -> t._3).toMap
    val localFilters = locals.result()

    // Log filter pushdown decisions
    if (_pushedPredicates.nonEmpty || localFilters.nonEmpty) {
      if (_pushedPredicates.nonEmpty) {
        val pushed = _pushedPredicates.map(formatPredicate).mkString(", ")
        logInfo(s"Filters pushed to API: $pushed")
      }
      if (localFilters.nonEmpty) {
        val local = localFilters.map(formatPredicate).mkString(", ")
        logInfo(s"Filters applied locally by Spark: $local")
      }
    }

    localFilters
  }

  def pushedPredicates(): Array[Predicate] = _pushedPredicates

  /** The query parameters this builder decided to send. Test-only accessor. */
  private[spark] def pushedParamsForTest: Map[String, String] = pushedParams

  def pushLimit(limit: Int): Boolean = {
    pushedLimit = Some(limit)
    // Return false: Spark should still apply limit post-scan since
    // pagination may return more rows than requested
    false
  }

  private def matchPredicate(predicate: Predicate): Option[(String, String)] = {
    val operator = predicate.name() match {
      case "="  => Some("eq")
      case ">"  => Some("gt")
      case ">=" => Some("gte")
      case "<"  => Some("lt")
      case "<=" => Some("lte")
      case "<>" => Some("neq")
      case _    => None
    }

    operator.flatMap { op =>
      val children = predicate.children()
      if (children.length == 2) {
        (children(0), children(1)) match {
          case (ref: NamedReference, lit: Literal[_]) =>
            val column = ref.fieldNames().mkString(".")
            val value = literalToString(lit)
            value.flatMap(findParam(column, op, _))
          case _ => None
        }
      } else None
    }
  }

  private def literalToString(lit: Literal[_]): Option[String] = {
    import java.time.{Instant, LocalDate, ZoneOffset}
    import java.time.format.DateTimeFormatter

    val isoFormatter = DateTimeFormatter.ISO_INSTANT

    lit.value() match {
      case null      => None
      case s: String => Some(s)
      case n: Number =>
        // Check if this is a date/timestamp type by examining the dataType
        val dataType = lit.dataType().typeName.toLowerCase
        if (dataType.contains("timestamp")) {
          // Spark timestamps are microseconds since epoch
          val micros = n.longValue()
          val instant = Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000)
          Some(isoFormatter.format(instant))
        } else if (dataType.contains("date")) {
          // Spark dates are days since epoch
          val days = n.intValue()
          val date = LocalDate.ofEpochDay(days)
          Some(date.atStartOfDay(ZoneOffset.UTC).format(isoFormatter))
        } else {
          Some(n.toString)
        }
      case other     => Some(other.toString)
    }
  }

  private def findParam(column: String, operator: String, value: String): Option[(String, String)] = {
    filterConfigs.find { fc =>
      fc.column == column && fc.operators.contains(operator)
    }.map(fc => fc.param -> value)
  }

  /** Format a predicate for human-readable logging. */
  private def formatPredicate(p: Predicate): String = {
    val children = p.children()
    if (children.length == 2) {
      (children(0), children(1)) match {
        case (ref: NamedReference, lit: Literal[_]) =>
          val column = ref.fieldNames().mkString(".")
          val value = Option(lit.value()).map {
            case s: String => s"'$s'"
            case other     => other.toString
          }.getOrElse("NULL")
          s"$column ${p.name()} $value"
        case _ => p.toString
      }
    } else p.toString
  }
}

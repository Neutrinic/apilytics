package com.apilytics.spark

import com.apilytics.core.config.FilterConfig
import org.apache.spark.sql.connector.expressions.{Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate

/** Shared filter and limit pushdown logic for scan builders.
  * Both RESTScanBuilder and ExplodedArrayScanBuilder mix in this trait
  * to map Spark SQL predicates to API query parameters.
  */
trait FilterPushdown {

  protected def filterConfigs: List[FilterConfig]

  protected var pushedParams: Map[String, String] = Map.empty
  protected var pushedLimit: Option[Int] = None
  protected var _pushedPredicates: Array[Predicate] = Array.empty

  def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val results = predicates.map(p => p -> matchPredicate(p))
    _pushedPredicates = results.collect { case (p, Some(_)) => p }
    pushedParams = results.flatMap(_._2).toMap
    results.collect { case (p, None) => p }
  }

  def pushedPredicates(): Array[Predicate] = _pushedPredicates

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
    lit.value() match {
      case null      => None
      case s: String => Some(s)
      case n: Number => Some(n.toString)
      case other     => Some(other.toString)
    }
  }

  private def findParam(column: String, operator: String, value: String): Option[(String, String)] = {
    filterConfigs.find { fc =>
      fc.column == column && fc.operators.contains(operator)
    }.map(fc => fc.param -> value)
  }
}

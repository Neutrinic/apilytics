package com.apilytics.spark

import com.apilytics.core.config.FilterConfig
import org.apache.spark.sql.connector.expressions.{Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownLimit, SupportsPushDownV2Filters}

class RESTScanBuilder(table: RESTTable) extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownLimit {

  private var pushedParams: Map[String, String] = Map.empty
  private var pushedLimit: Option[Int] = None
  private var _pushedPredicates: Array[Predicate] = Array.empty

  private val filterConfigs: List[FilterConfig] =
    table.tableConfig.map(_.filters).getOrElse(Nil)

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val results = predicates.map(p => p -> matchPredicate(p))
    _pushedPredicates = results.collect { case (p, Some(_)) => p }
    pushedParams = results.flatMap(_._2).toMap
    results.collect { case (p, None) => p }
  }

  override def pushedPredicates(): Array[Predicate] = _pushedPredicates

  override def pushLimit(limit: Int): Boolean = {
    pushedLimit = Some(limit)
    // Return false: Spark should still apply limit post-scan since
    // pagination may return more rows than requested
    false
  }

  override def build(): Scan = new RESTScan(table, pushedParams, pushedLimit)

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
      case null       => None
      case s: String  => Some(s)
      case n: Number  => Some(n.toString)
      case other      => Some(other.toString)
    }
  }

  private def findParam(column: String, operator: String, value: String): Option[(String, String)] = {
    filterConfigs.find { fc =>
      fc.column == column && fc.operators.contains(operator)
    }.map(fc => fc.param -> value)
  }
}

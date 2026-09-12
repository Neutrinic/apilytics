package com.apilytics.spark

import com.apilytics.core.config.FilterConfig
import munit.FunSuite
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.unsafe.types.UTF8String

/** Predicate-to-query-parameter mapping (#243).
  *
  * A pushed predicate is a promise: Spark drops it from the plan and never re-checks it.
  * Anything the connector cannot faithfully represent in the request has to stay local,
  * because a broken promise here is a wrong answer rather than a slow one.
  */
class FilterPushdownSuite extends FunSuite {

  private def builder(configs: List[FilterConfig]) = new FilterPushdown {
    override protected def filterConfigs: List[FilterConfig] = configs
  }

  private def predicate(op: String, column: String, value: String): Predicate =
    new Predicate(op, Array[Expression](
      Expressions.column(column),
      Expressions.literal(UTF8String.fromString(value))
    ))

  test("a range on one parameter keeps both bounds or pushes neither") {
    // `created_at >= X AND created_at <= Y` both resolve to the same query parameter, and
    // one request cannot carry two values for it. Reporting both as pushed while sending
    // only one makes Spark skip a filter that was never applied anywhere.
    val b = builder(List(FilterConfig(param = "since", column = "created_at",
                                      operators = List("gte", "lte"))))

    val lower = predicate(">=", "created_at", "2026-01-01")
    val upper = predicate("<=", "created_at", "2026-06-01")
    val local = b.pushPredicates(Array(lower, upper))

    // The invariant: never claim more predicates than the request can carry values for.
    assertEquals(
      b.pushedPredicates().length + local.length, 2,
      "every predicate must be either pushed or returned for Spark to apply"
    )
    assertEquals(
      b.pushedPredicates().length, b.pushedParamsForTest.get("since").size,
      s"claimed ${b.pushedPredicates().length} predicates pushed but the request carries " +
        s"${b.pushedParamsForTest.get("since").size} value(s) for 'since'"
    )

    // And the specific outcome, so this cannot pass by pushing the wrong bound or by
    // giving up on both. First claimant wins, which keeps the narrowing it buys.
    assertEquals(b.pushedPredicates().toList, List(lower), "the first bound should be the pushed one")
    assertEquals(local.toList, List(upper), "the colliding bound must go back to Spark")
    assertEquals(b.pushedParamsForTest, Map("since" -> "2026-01-01"))
  }

  test("predicates on distinct parameters all push") {
    val b = builder(List(
      FilterConfig(param = "state", column = "state", operators = List("eq")),
      FilterConfig(param = "author", column = "user", operators = List("eq"))
    ))

    val local = b.pushPredicates(Array(
      predicate("=", "state", "open"),
      predicate("=", "user", "octocat")
    ))

    assertEquals(local.length, 0)
    assertEquals(b.pushedPredicates().length, 2)
    assertEquals(b.pushedParamsForTest, Map("state" -> "open", "author" -> "octocat"))
  }

  test("an unmatched predicate stays local") {
    val b = builder(List(FilterConfig(param = "state", column = "state", operators = List("eq"))))
    val local = b.pushPredicates(Array(predicate("=", "nope", "x")))
    assertEquals(local.length, 1)
    assertEquals(b.pushedPredicates().length, 0)
  }
}

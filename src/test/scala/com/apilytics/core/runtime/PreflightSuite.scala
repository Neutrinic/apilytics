package com.apilytics.core.runtime

import munit.FunSuite

/** A clash must become an immediate error naming the library, never a hang (#264). */
class PreflightSuite extends FunSuite {

  import Preflight.Probe

  test("the real probes pass on this classpath") {
    assertEquals(Preflight.firstFailure(Preflight.probes), None)
    Preflight.verify()
  }

  test("a linkage error names the library and the underlying error") {
    // What DBR 18's older cats produced when http4s initialised.
    val clash = Probe("http4s", () => throw new NoSuchMethodError("'cats.kernel.Order$ cats.package$.Order()'"))
    val msg = Preflight.firstFailure(Seq(Probe("fine", () => ()), clash)).getOrElse(fail("no failure reported"))

    assert(msg.contains("http4s failed to initialise"), msg)
    assert(msg.contains("java.lang.NoSuchMethodError"), msg)
    assert(msg.contains("cats.package$.Order()"), msg)
  }

  test("the first failing probe is the one reported") {
    val msg = Preflight.firstFailure(Seq(
      Probe("first", () => throw new NoClassDefFoundError("a/B")),
      Probe("second", () => throw new NoSuchMethodError("c.D"))
    ))
    assert(msg.exists(_.contains("first failed")), msg.toString)
  }

  test("out-of-memory and similar are rethrown, not reworded as a classpath problem") {
    // munit's intercept passes fatal errors straight through, so catch by hand.
    val rethrown =
      try { Preflight.firstFailure(Seq(Probe("x", () => throw new OutOfMemoryError("boom")))); false }
      catch { case _: OutOfMemoryError => true }
    assert(rethrown, "an OutOfMemoryError must propagate")
  }
}

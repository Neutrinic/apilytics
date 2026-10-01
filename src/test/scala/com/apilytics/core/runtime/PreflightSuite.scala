package com.apilytics.core.runtime

import munit.FunSuite

import scala.concurrent.duration._

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

  test("a probe that never finishes is reported as a failure, not waited on forever") {
    // A clash fatal inside a cats-effect fiber does not throw; the fiber dies and its
    // caller waits indefinitely. The check must report that, not become the hang.
    val never = new java.util.concurrent.CountDownLatch(1)
    val started = System.nanoTime()
    val msg = Preflight.firstFailure(Seq(Probe("stuck", () => never.await())), 200.millis)
    val waited = (System.nanoTime() - started) / 1000000

    assert(msg.exists(_.contains("stuck did not finish initialising within 200 milliseconds")), msg.toString)
    assert(waited < 5000, s"took ${waited}ms; the probe was waited on past its timeout")
  }

  test("out-of-memory and similar are rethrown, not reworded as a classpath problem") {
    // munit's intercept passes fatal errors straight through, so catch by hand.
    val rethrown =
      try { Preflight.firstFailure(Seq(Probe("x", () => throw new OutOfMemoryError("boom")))); false }
      catch { case _: OutOfMemoryError => true }
    assert(rethrown, "an OutOfMemoryError must propagate")
  }
}

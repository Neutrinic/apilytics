package com.apilytics.core.http

import cats.effect.{IO, Ref, Resource}
import cats.effect.unsafe.implicits.global
import com.apilytics.core.config.{AuthConfig, AuthType, HttpConfig}
import munit.FunSuite
import org.http4s.{Method, Request, Response, Status, Uri}

import java.net.SocketTimeoutException
import java.util.concurrent.TimeoutException
import scala.concurrent.duration._

/** A request to an unreachable host fails within the configured timeout.
  *
  * On EMR Serverless, which has no internet access without a VPC, each attempt to reach a
  * public API waited for the operating system's TCP connect timeout — about two minutes on
  * Linux — rather than `http.timeout`, and with retries a task took 14 minutes to fail.
  * The connection phase has to be bounded by the same timeout as the request.
  */
class ConnectTimeoutSuite extends FunSuite {

  // Stands in for a connection that never opens: acquisition never completes.
  private val stalled = org.http4s.client.Client[IO](_ => Resource.eval(IO.never))

  private def get(uri: String) = Request[IO](Method.GET, Uri.unsafeFromString(uri))

  private def timed[A](io: IO[A]): (Either[Throwable, A], Long) = {
    val started = System.nanoTime()
    val outcome = io.attempt.unsafeRunTimed(60.seconds).getOrElse(fail("did not finish in 60s"))
    (outcome, (System.nanoTime() - started) / 1000000)
  }

  private def causes(e: Throwable): List[Throwable] =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(10).toList

  private def isBoundedTimeout(e: Throwable) = causes(e).exists {
    case t: SocketTimeoutException => t.getMessage.startsWith("No response from")
    case _                          => false
  }

  test("a stalled acquisition fails with SocketTimeoutException at the configured timeout") {
    val (outcome, tookMs) =
      timed(Client.boundedAcquire(stalled, 500.millis).run(get("http://stalled.test/x")).use_)

    assert(outcome.left.exists(_.isInstanceOf[SocketTimeoutException]), s"got $outcome")
    assert(tookMs < 3000, s"took ${tookMs}ms with a 500ms timeout")
  }

  test("the failure names the host and path, but no credentials") {
    val (outcome, _) = timed(
      Client.boundedAcquire(stalled, 200.millis)
        .run(get("http://user:hunter2@stalled.test:81/unreachable?api_key=secret"))
        .use_
    )
    val message = outcome.left.toOption.getOrElse(fail("expected a failure")).getMessage

    assert(message.contains("stalled.test:81/unreachable"), message)
    assert(!message.contains("secret"), "query parameters must not appear in the error")
    assert(!message.contains("hunter2") && !message.contains("user"), "user info must not appear in the error")
  }

  test("the caller can cancel a stalled acquisition before the timeout") {
    val (outcome, tookMs) = timed(
      Client.boundedAcquire(stalled, 30.seconds).run(get("http://stalled.test/x")).use_.timeout(300.millis)
    )

    assert(outcome.left.exists(_.isInstanceOf[TimeoutException]), s"got $outcome")
    assert(tookMs < 5000, s"cancellation waited ${tookMs}ms: acquisition is not cancellable")
  }

  test("a response that arrives in time is passed through and released after use") {
    val (outcome, _) = timed(for {
      released <- Ref.of[IO, Boolean](false)
      responding = org.http4s.client.Client[IO](_ =>
        Resource.make(IO.pure(Response[IO](Status.Ok)))(_ => released.set(true)))
      status <- Client.boundedAcquire(responding, 5.seconds).run(get("http://ok.test/x")).use(r => IO.pure(r.status))
      after <- released.get
    } yield (status, after))

    assertEquals(outcome, Right((Status.Ok, true)))
  }

  // End to end through the real ember clients, so dropping boundedAcquire from either is
  // caught. Non-routable: on most networks packets to it are dropped, so a connect attempt
  // waits as it would without egress. A network that rejects it at once cannot exercise
  // the connect phase, so the test is skipped there rather than passing for the wrong reason.
  private val blackHole = "http://10.255.255.1:81/unreachable"

  private def assertBounded(outcome: Either[Throwable, _], tookMs: Long): Unit = {
    val err = outcome.left.toOption.getOrElse(fail(s"expected a failure, got $outcome"))
    assert(tookMs < 8000, s"took ${tookMs}ms with a 2s timeout: the connect phase is not bounded")
    assume(isBoundedTimeout(err), s"the network rejected $blackHole at once ($err); connect phase not exercised")
  }

  test("an unreachable host fails within the configured timeout") {
    val cfg = HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 2.seconds)
    val (outcome, tookMs) = timed(
      Client.resource(cfg, AuthConfig(authType = AuthType.None)).use(_.get(Uri.unsafeFromString(blackHole)))
    )
    assertBounded(outcome, tookMs)
  }

  test("an unreachable OAuth2 token endpoint fails within the configured timeout") {
    val (outcome, tookMs) = timed(
      OAuth2TokenManager.resource("id", "secret", "http://10.255.255.1:81/token", 2.seconds).use(_.getToken)
    )
    assertBounded(outcome, tookMs)
  }
}

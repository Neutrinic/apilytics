package com.apilytics.core.runtime

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._

/** Fails fast, and says why, when this JVM's classpath cannot run apilytics' HTTP stack.
  *
  * A library apilytics depends on can be shadowed by another copy that the platform loads
  * first. The failure then surfaces as a LinkageError (NoSuchMethodError,
  * NoClassDefFoundError) the first time the library is used — inside a cats-effect fiber,
  * where it is fatal: it bypasses every error handler, the reader waits forever for a batch
  * that never comes, and the task hangs until the platform kills the executor. That is how
  * an older cats on Databricks DBR 18 presented (#264).
  *
  * The published jar relocates its dependencies, so this should never fire. It exists so
  * that whatever clash the shading does not cover is an immediate error naming the library,
  * not a hang. The probes initialise, in plain JVM code where a LinkageError can be caught,
  * the classes a read uses first. The result is computed once per JVM.
  */
object Preflight {

  private[apilytics] final case class Probe(library: String, run: () => Unit)

  private[apilytics] val probes: Seq[Probe] = Seq(
    Probe("http4s", () => { org.http4s.Uri.unsafeFromString("https://example.com/p?q=1"); () }),
    Probe("http4s-ember-client", () => { org.http4s.ember.client.EmberClientBuilder.default[IO]; () }),
    Probe("circe", () => { io.circe.parser.parse("""{"a":[1]}""").fold(e => throw e, _ => ()) }),
    Probe("cats-effect / fs2", () => { fs2.Stream.emit(1).covary[IO].compile.toList.unsafeRunSync(); () })
  )

  private lazy val failure: Option[String] = firstFailure(probes)

  /** Throws IllegalStateException if the probes failed in this JVM. */
  def verify(): Unit = failure.foreach(msg => throw new IllegalStateException(msg))

  /** How long one probe may take. They are trivial; this only bounds one that never ends. */
  private[apilytics] val probeTimeout: FiniteDuration = 30.seconds

  private[apilytics] def firstFailure(ps: Seq[Probe], timeout: FiniteDuration = probeTimeout): Option[String] =
    ps.view.flatMap(p => outcome(p, timeout)).headOption

  /** Runs one probe on its own thread, for at most `timeout`.
    *
    * A clash that is fatal inside a cats-effect fiber does not throw: the fiber dies and
    * whoever waits on it waits forever — the hang this object exists to prevent. A class
    * initialiser can stall the same way. So no probe is waited on unbounded, and one that
    * does not finish is reported as a failure like any other. The thread is a daemon, so an
    * abandoned probe cannot keep the JVM alive.
    */
  private def outcome(p: Probe, timeout: FiniteDuration): Option[String] = {
    val result = new AtomicReference[Either[Throwable, Unit]]()
    val t = new Thread(
      () => result.set(try Right(p.run()) catch { case e: Throwable => Left(e) }),
      s"apilytics-preflight-${p.library}"
    )
    t.setDaemon(true)
    t.start()
    t.join(timeout.toMillis)
    Option(result.get()) match {
      case None                                => Some(stalled(p.library, timeout))
      case Some(Left(vm: VirtualMachineError)) => throw vm // out of memory and the like: not ours to reword
      case Some(Left(e))                       => Some(describe(p.library, e))
      case Some(Right(_))                      => None
    }
  }

  private def stalled(library: String, timeout: FiniteDuration): String =
    s"apilytics cannot run on this classpath: $library did not finish initialising within " +
      s"$timeout. A library apilytics uses may be clashing with another copy loaded ahead of " +
      "its own; check for other copies of this library on the classpath."

  private def describe(library: String, t: Throwable): String =
    s"apilytics cannot run on this classpath: $library failed to initialise " +
      s"(${t.getClass.getName}: ${t.getMessage}). Another version of a library apilytics " +
      "uses is loaded ahead of its own. The published apilytics jar bundles its dependencies " +
      "relocated; if you built or repackaged it, keep that relocation, and check for other " +
      "copies of this library on the classpath."
}

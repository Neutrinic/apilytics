package com.apilytics.core.source

import cats.effect.{IO, Resource}
import com.apilytics.core.checkpoint.CheckpointState
import fs2.Stream
import io.circe.Json

/** Protocol-neutral read interface.
  *
  * The Spark layer talks to this and never to a protocol's own types, so adding a
  * protocol touches nothing under `spark/` (see #191). Each protocol supplies its own
  * [[RecordSource]]; REST's lives in `com.apilytics.core.rest`.
  */
/** Opaque, protocol-specific identity of one readable table.
  *
  * The Spark layer carries this around without inspecting it — it travels inside an
  * InputPartition, hence `Serializable`. Only the protocol that produced a handle
  * knows how to read it.
  */
trait SourceHandle extends Serializable

/** What to read, including everything Spark managed to push down. */
final case class ReadRequest(
    handle: SourceHandle,
    /** Filters and other parameters pushed down by the planner. */
    params: Map[String, String] = Map.empty,
    /** Row limit pushed down by the planner, if any. */
    limit: Option[Int] = None,
    /** Checkpoint position to resume from, for incremental reads. */
    startState: Option[CheckpointState] = None
) extends Serializable

/** A batch of records, plus the checkpoint position the source reached producing it.
  *
  * "Page" is the source's natural unit of retrieval, not Spark's batch size — the
  * reader re-chunks these into Arrow batches of its own choosing.
  */
final case class RecordPage(records: List[Json], state: Option[CheckpointState])

/** A live connection to a source. Obtained from [[RecordSource.session]]. */
trait RecordSession {

  /** Stream the records matching `request`.
    *
    * Lazy: pages are fetched as the stream is pulled, so a caller that stops early
    * stops the underlying I/O.
    */
  def pages(request: ReadRequest): Stream[IO, RecordPage]
}

/** Opens sessions against a source.
  *
  * `Serializable` because it is constructed on the driver and used on executors.
  * Configuration belongs here; per-read concerns belong in [[ReadRequest]].
  */
trait RecordSource extends Serializable {

  /** Acquire a session. Released when the resource is finalised. */
  def session: Resource[IO, RecordSession]
}

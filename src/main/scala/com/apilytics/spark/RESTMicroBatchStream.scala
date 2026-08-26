package com.apilytics.spark

import com.apilytics.core.checkpoint.CheckpointState
import com.apilytics.core.config.CheckpointConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SupportsTriggerAvailableNow}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset}

/** Exclusive upper bound applied to a streaming batch.
  *
  * The API filter is one-sided — `?since=<start>` — so a request also returns records
  * newer than the batch's end offset, which the next batch would return again. Trimming
  * them here is what keeps consecutive batches from overlapping.
  *
  * Records are compared as instants, not as strings. String order disagrees with time
  * order whenever the two sides differ in precision: `'.'` sorts below `'Z'`, so
  * `2026-01-15T10:00:00Z` compares as *not* earlier than `2026-01-15T10:00:00.5Z`. A
  * record trimmed that way is then skipped by the next batch's `since` and lost for good.
  * API timestamp precision is not ours to control, so the comparison cannot assume it.
  */
final case class StreamBound(timestampPath: String, endExclusive: String) extends Serializable

/** Streaming position, expressed as a timestamp.
  *
  * Serialised through `CheckpointState` so a stream offset and a batch checkpoint are the
  * same shape on disk and can be read by the same code.
  */
final case class TimestampOffset(value: String) extends Offset {
  override def json(): String = CheckpointState.TimestampValue(value).toJson.noSpaces
}

object TimestampOffset {
  def fromJson(json: String): TimestampOffset =
    CheckpointState.fromJsonString(json) match {
      case Right(CheckpointState.TimestampValue(v)) => TimestampOffset(v)
      case Right(other) =>
        throw new IllegalArgumentException(
          s"Expected a timestamp offset, got ${other.getClass.getSimpleName}. " +
            "A stream checkpoint written by a different checkpoint mode cannot be resumed."
        )
      case Left(err) => throw new IllegalArgumentException(s"Malformed stream offset: $err")
    }
}

/** Micro-batch source over a REST endpoint.
  *
  * Only timestamp checkpointing can drive this. `latestOffset()` has to answer "how far
  * could I read right now" without reading, and for a pull-based API the sole honest
  * answer is the clock — which is meaningful only when the API accepts a
  * "changed since" parameter. Cursor and offset modes cannot say how much is available
  * without fetching it, so `RESTTable` does not advertise MICRO_BATCH_READ for them.
  *
  * Delivery is at-least-once. A record written with a timestamp at or before the current
  * offset, but not visible to the API until after that batch ran, is missed; one visible
  * in two batches is trimmed by `StreamBound`. APIs with non-monotonic visibility need a
  * lag applied at the source.
  */
class RESTMicroBatchStream(
    scan: RESTScan,
    checkpoint: CheckpointConfig,
    timestampParam: String
) extends MicroBatchStream
    with SupportsTriggerAvailableNow
    with Logging {

  /** Fixed-width, second precision.
    *
    * `ISO_INSTANT` renders whatever precision the clock has — `10:00:00.478951900Z` on a
    * modern JVM — which is both an odd thing to hand an API as `?since=` and a value that
    * misorders against second-precision data under string comparison. Truncating keeps the
    * wire format stable and predictable.
    */
  private val formatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC)

  private def now(): String = formatter.format(Instant.now().truncatedTo(ChronoUnit.SECONDS))

  /** Where a fresh stream starts: now, so only new records are delivered.
    *
    * Starting at the epoch would replay the entire history of the endpoint on the first
    * batch, which for a rate-limited API is the difference between a stream that starts
    * and one that does not. Use a batch query to load history.
    */
  override def initialOffset(): Offset = {
    val start = now()
    logInfo(s"Streaming from $start (new records only)")
    TimestampOffset(start)
  }

  override def latestOffset(): Offset = TimestampOffset(availableNowEnd.getOrElse(now()))

  /** End of the current Trigger.AvailableNow run, fixed when the run is prepared.
    *
    * "Available now" has to mean a specific instant. Left reading the clock, the end
    * keeps moving and a run that is supposed to drain and stop instead chases records
    * that arrive while it works.
    */
  @volatile private var availableNowEnd: Option[String] = None

  override def prepareForTriggerAvailableNow(): Unit = {
    val end = now()
    logInfo(s"Trigger.AvailableNow: draining up to $end")
    availableNowEnd = Some(end)
  }

  /** Called instead of the no-argument form once the source declares admission control.
    *
    * There is no rate limit to apply: a batch is one request for whatever the API says
    * changed in the window, and `ReadLimit` cannot be translated into that.
    */
  override def latestOffset(start: Offset, limit: ReadLimit): Offset = latestOffset()

  override def deserializeOffset(json: String): Offset = TimestampOffset.fromJson(json)

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val from = start.asInstanceOf[TimestampOffset].value
    val to   = end.asInstanceOf[TimestampOffset].value

    // Nothing can have happened in an empty interval, so skip the request entirely.
    if (from >= to) Array.empty
    else {
      // timestamp-path is required to advertise streaming at all, so the bound is always
      // available here; without it every batch would re-deliver the previous one's tail.
      val bound = checkpoint.timestampPath.map(StreamBound(_, to))
      scan.streamingPartitions(timestampParam, from, bound)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = scan.createReaderFactory()

  /** Spark owns the offset log, so there is nothing of ours to durably advance here. */
  override def commit(end: Offset): Unit = ()

  override def stop(): Unit = ()
}

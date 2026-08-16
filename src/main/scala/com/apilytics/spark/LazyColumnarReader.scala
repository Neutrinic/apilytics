package com.apilytics.spark

import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import com.apilytics.core.source.{RecordSession, RecordSource}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.jdk.CollectionConverters._

/** Base class for columnar readers that lazily stream Arrow batches via a bounded queue.
  *
  * A background fiber runs the fs2 pagination stream into a bounded queue.
  * Spark's synchronous next()/get()/close() interface dequeues one batch at a time,
  * so peak memory scales with batch size, not total partition size.
  */
abstract class LazyColumnarReader extends PartitionReader[ColumnarBatch] {

  // Subclasses provide these:
  protected def allocator: RootAllocator
  protected def arrowSchema: ArrowSchema

  /** Where records come from. Protocol-neutral — this base class knows nothing about
    * HTTP, pagination or OpenAPI, so a non-REST source drops in unchanged (#191).
    */
  protected def recordSource: RecordSource
  protected def buildStream(session: RecordSession): fs2.Stream[IO, (ColumnarBatch, VectorSchemaRoot)]

  /** Batches held in flight ahead of Spark. Must be >= 1.
    *
    * Overrides read this from the partition's config, so keep it a `def` reading only
    * constructor params — subclass `val`s are still unset when this base constructor runs.
    */
  protected def prefetchSize: Int = 2

  // --- Lifecycle managed by this base class ---

  // Acquire the session manually so it stays open across next() calls.
  private val (session, releaseSession) = recordSource.session.allocated.unsafeRunSync()

  // Queue transports Either so producer errors propagate to the Spark thread.
  // None = end-of-stream sentinel, Some(Left(t)) = error, Some(Right(...)) = batch.
  private type QueueItem = Option[Either[Throwable, (ColumnarBatch, VectorSchemaRoot)]]

  private val queue: Queue[IO, QueueItem] =
    Queue.bounded[IO, QueueItem](prefetchSize).unsafeRunSync()

  // Producer fiber: runs the stream in background, feeding batches into the queue.
  private val producer = buildStream(session)
    .evalMap(batch => queue.offer(Some(Right(batch))))
    .compile
    .drain
    .handleErrorWith(t => queue.offer(Some(Left(t))))
    .guarantee(queue.offer(None)) // sentinel: always sent after success or error
    .start
    .unsafeRunSync()

  private var currentBatch: ColumnarBatch = _
  private var currentRoot: VectorSchemaRoot = _

  override def next(): Boolean = {
    queue.take.unsafeRunSync() match {
      case Some(Right((batch, root))) =>
        // Release the previous batch before storing the new one
        if (currentBatch != null) currentBatch.close()
        if (currentRoot != null) currentRoot.close() // null for variant path (no Arrow)
        currentBatch = batch
        currentRoot = root
        true
      case Some(Left(t)) =>
        throw t // Re-throw on the Spark thread so the query fails visibly
      case None =>
        false
    }
  }

  override def get(): ColumnarBatch = currentBatch

  /** High-water mark of Arrow memory held by this reader, in bytes.
    *
    * Exposed so tests can assert the bounded-queue contract: peak tracks
    * `prefetch-batches * arrow-batch-size`, not the size of the partition.
    */
  private[apilytics] def peakAllocatedBytes: Long = allocator.getPeakMemoryAllocation

  override def close(): Unit = {
    // 1. Cancel the producer, draining concurrently so cancellation can complete.
    //
    // The producer's `guarantee` finalizer offers the end-of-stream sentinel, and
    // `guarantee` finalizers are uncancelable. `Fiber.cancel` waits for finalization.
    // So if the queue is full when Spark stops early — a satisfied LIMIT, a failed
    // task — that offer blocks forever and `cancel` never returns, deadlocking
    // close(). Draining alongside cancellation keeps a slot free for the sentinel.
    (for {
      drainer <- drainLoop.start
      _       <- producer.cancel
      _       <- drainer.cancel
    } yield ()).unsafeRunSync()

    // 2. Sweep anything offered between the drainer stopping and now
    drainQueue()

    // 3. Close current batch + root
    if (currentBatch != null) currentBatch.close()
    if (currentRoot != null) currentRoot.close()

    // 4. Release the source session
    releaseSession.unsafeRunSync()

    // 5. Close Arrow allocator (verifies all memory released)
    allocator.close()
  }

  /** Continuously take and release queued batches until cancelled.
    *
    * Runs only during close(), to keep the bounded queue from blocking the producer's
    * uncancelable finalizer. Batches taken here are released rather than handed on —
    * close() means Spark is done reading.
    */
  private def drainLoop: IO[Unit] =
    queue.take.flatMap { item =>
      IO {
        item.foreach {
          case Right((batch, root)) =>
            batch.close()
            if (root != null) root.close()
          case Left(_) => // errors are irrelevant once we are closing
        }
      }.flatMap(_ => drainLoop)
    }

  private def drainQueue(): Unit = {
    var item = queue.tryTake.unsafeRunSync()
    while (item.isDefined) {
      item.flatten.foreach {
        case Right((batch, root)) =>
          batch.close()
          if (root != null) root.close() // null for variant path (no Arrow)
        case Left(_) => // discard errors during drain
      }
      item = queue.tryTake.unsafeRunSync()
    }
  }

  /** Wrap Arrow VectorSchemaRoot as ColumnarBatch with null-safe column vector wrappers.
    *
    * We use NullSafeArrowColumnVector instead of Spark's ArrowColumnVector because
    * Arrow's underlying vector.get() throws IllegalStateException on null values.
    * Our wrapper checks isNull() before accessing values.
    */
  protected def arrowToBatch(root: VectorSchemaRoot): ColumnarBatch = {
    val vectors = root.getFieldVectors.asScala.map { v =>
      NullSafeArrowColumnVector(v)
    }.toArray
    val batch = new ColumnarBatch(vectors.asInstanceOf[Array[org.apache.spark.sql.vectorized.ColumnVector]])
    batch.setNumRows(root.getRowCount)
    batch
  }
}

package com.apilytics.spark

import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import com.apilytics.core.arrow.ConversionStats
import com.apilytics.core.source.{RecordSession, RecordSource}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.jdk.CollectionConverters._

/** Base class for columnar readers that lazily stream Arrow batches via a bounded queue.
  *
  * A background fiber runs the fs2 pagination stream into a bounded queue.
  * Spark's synchronous next()/get()/close() interface dequeues one batch at a time,
  * so peak memory scales with batch size, not total partition size.
  */
abstract class LazyColumnarReader extends PartitionReader[ColumnarBatch] with Logging {

  /** What conversion did to this task's values: what was converted, what became NULL. The
    * first NULL in each column is logged; all of them are counted in the Spark UI (#309).
    */
  protected val conversionStats: ConversionStats = new ConversionStats(msg => logWarning(msg))

  override def currentMetricsValues(): Array[CustomTaskMetric] =
    ConversionMetrics.taskValues(conversionStats)

  // Subclasses provide these:
  protected def allocator: RootAllocator
  protected def arrowSchema: ArrowSchema

  /** Where records come from. Protocol-neutral — this base class knows nothing about
    * HTTP, pagination or OpenAPI, so a non-REST source drops in unchanged (#191).
    */
  protected def recordSource: RecordSource
  protected def buildStream(session: RecordSession): fs2.Stream[IO, (ColumnarBatch, VectorSchemaRoot)]

  /** Batches held in flight ahead of Spark. Must be >= 1. Read when the reader starts, on
    * the first next(), by which time the subclass is fully constructed. */
  protected def prefetchSize: Int = 2

  // --- Lifecycle managed by this base class ---

  // Queue transports Either so producer errors propagate to the Spark thread.
  // None = end-of-stream sentinel, Some(Left(t)) = error, Some(Right(...)) = batch.
  private type QueueItem = Option[Either[Throwable, (ColumnarBatch, VectorSchemaRoot)]]

  /** The session, queue and producer fiber of a reader that has started. */
  private final class Running(
      val releaseSession: IO[Unit],
      val queue: Queue[IO, QueueItem],
      val producer: cats.effect.FiberIO[Unit]
  )

  /** Started on the first next(), never in this constructor.
    *
    * A base-class constructor runs before the subclass's fields are assigned. Starting the
    * producer here let it run against a subclass whose `arrowSchema` and `allocator` were
    * still null, or never visibly set at all: nothing ordered those writes with the fiber's
    * reads. It showed up as an intermittent NullPointerException on a null Arrow schema
    * (#262). By the first next() the subclass is fully constructed. Spark calls next()
    * straight after creating a reader, so prefetching starts no later in practice.
    *
    * Touched only from the task thread that owns this reader, so no synchronisation.
    */
  private var running: Running = _

  private def start(): Running = {
    if (running == null) {
      // Acquire the session manually so it stays open across next() calls.
      val (session, release) = recordSource.session.allocated.unsafeRunSync()
      // Until `running` is set, close() cannot see the session, so this block owns it: if
      // anything below throws — buildStream does real work while building, and runs on the
      // caller's thread where the stream's own handleErrorWith cannot catch it — release
      // the session here before rethrowing.
      try {
        val queue = Queue.bounded[IO, QueueItem](prefetchSize).unsafeRunSync()
        // Producer fiber: runs the stream in background, feeding batches into the queue.
        val producer = buildStream(session)
          .evalMap(batch => queue.offer(Some(Right(batch))))
          .compile
          .drain
          .handleErrorWith(t => queue.offer(Some(Left(t))))
          .guarantee(queue.offer(None)) // sentinel: always sent after success or error
          .start
          .unsafeRunSync()
        running = new Running(release, queue, producer)
      } catch {
        case t: Throwable =>
          try release.unsafeRunSync()
          catch { case r: Throwable => t.addSuppressed(r) }
          throw t
      }
    }
    running
  }

  private var currentBatch: ColumnarBatch = _
  private var currentRoot: VectorSchemaRoot = _

  /** Every Arrow batch allocated and not yet released, from `arrowToBatch` until it is
    * closed.
    *
    * close() cancels the producer, and the producer can be holding a batch the queue never
    * saw: converted, then parked offering it to a full queue, or between the two. Nothing
    * else refers to that batch, so it went unclosed and `allocator.close()` failed the task
    * with "Memory was leaked by query" (#307). A satisfied LIMIT, `show()` or `take()` hit it
    * routinely. close() releases whatever is still here before closing the allocator, so no
    * allocated batch can be dropped, wherever cancellation lands.
    *
    * Written by the producer fiber and the task thread, hence concurrent. Keyed by the
    * batch, which compares by identity.
    */
  private val inFlight = new java.util.concurrent.ConcurrentHashMap[ColumnarBatch, VectorSchemaRoot]()

  /** Close a batch and its root, once. Variant batches have no root and aren't tracked. */
  private def release(batch: ColumnarBatch, root: VectorSchemaRoot): Unit =
    if (root == null) batch.close()
    else if (inFlight.remove(batch) != null) {
      batch.close()
      root.close()
    }

  override def next(): Boolean = {
    start().queue.take.unsafeRunSync() match {
      case Some(Right((batch, root))) =>
        // Release the previous batch before storing the new one
        if (currentBatch != null) release(currentBatch, currentRoot)
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
    //
    // A reader closed before its first next() never started anything, so there is
    // nothing to cancel, drain or release.
    val r = running
    if (r != null) {
      (for {
        drainer <- drainLoop(r.queue).start
        _       <- r.producer.cancel
        _       <- drainer.cancel
      } yield ()).unsafeRunSync()

      // 2. Sweep anything offered between the drainer stopping and now
      drainQueue(r.queue)
    }

    // 3. Close current batch + root
    if (currentBatch != null) release(currentBatch, currentRoot)

    // 4. Close any batch the producer allocated but never queued, or the drainer took but
    // was cancelled before closing. The producer and drainer have stopped, so this is final.
    inFlight.asScala.toList.foreach { case (batch, root) => release(batch, root) }

    // 5. Release the source session
    if (r != null) r.releaseSession.unsafeRunSync()

    // 6. Close Arrow allocator (verifies all memory released)
    allocator.close()
  }

  /** Continuously take and release queued batches until cancelled.
    *
    * Runs only during close(), to keep the bounded queue from blocking the producer's
    * uncancelable finalizer. Batches taken here are released rather than handed on —
    * close() means Spark is done reading.
    */
  private def drainLoop(queue: Queue[IO, QueueItem]): IO[Unit] =
    queue.take.flatMap { item =>
      IO {
        item.foreach {
          case Right((batch, root)) => release(batch, root)
          case Left(_) => // errors are irrelevant once we are closing
        }
      }.flatMap(_ => drainLoop(queue))
    }

  private def drainQueue(queue: Queue[IO, QueueItem]): Unit = {
    var item = queue.tryTake.unsafeRunSync()
    while (item.isDefined) {
      item.flatten.foreach {
        case Right((batch, root)) => release(batch, root)
        case Left(_) => // discard errors during drain
      }
      item = queue.tryTake.unsafeRunSync()
    }
  }

  /** Wrap Arrow VectorSchemaRoot as ColumnarBatch with null-safe column vector wrappers.
    *
    * Call it as soon as `root` is allocated: it registers the pair, so close() releases it
    * even if the producer is cancelled before the batch reaches the queue (#307).
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
    inFlight.put(batch, root)
    batch
  }
}

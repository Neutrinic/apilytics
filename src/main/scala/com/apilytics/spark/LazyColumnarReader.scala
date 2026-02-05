package com.apilytics.spark

import cats.effect.{IO, Resource}
import cats.effect.std.{Dispatcher, Queue}
import cats.effect.unsafe.implicits.global
import com.apilytics.core.http.Client
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

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
  protected def clientResource: Resource[IO, Client.RestClient]
  protected def buildStream(client: Client.RestClient): fs2.Stream[IO, (ColumnarBatch, VectorSchemaRoot)]
  protected def prefetchSize: Int = 2

  // --- Lifecycle managed by this base class ---

  private val (dispatcher, releaseDispatcher) =
    Dispatcher.sequential[IO].allocated.unsafeRunSync()

  private val (client, releaseClient) =
    dispatcher.unsafeRunSync(clientResource.allocated)

  private val queue: Queue[IO, Option[(ColumnarBatch, VectorSchemaRoot)]] =
    dispatcher.unsafeRunSync(Queue.bounded[IO, Option[(ColumnarBatch, VectorSchemaRoot)]](prefetchSize))

  private val producer = dispatcher.unsafeRunSync {
    buildStream(client)
      .evalMap(batch => queue.offer(Some(batch)))
      .compile
      .drain
      .guarantee(queue.offer(None)) // sentinel: always sent on success or error
      .start
  }

  private var currentBatch: ColumnarBatch = _
  private var currentRoot: VectorSchemaRoot = _

  override def next(): Boolean = {
    dispatcher.unsafeRunSync(queue.take) match {
      case Some((batch, root)) =>
        // Release the previous batch before storing the new one
        if (currentBatch != null) currentBatch.close()
        if (currentRoot != null) currentRoot.close()
        currentBatch = batch
        currentRoot = root
        true
      case None =>
        false
    }
  }

  override def get(): ColumnarBatch = currentBatch

  override def close(): Unit = {
    // 1. Cancel producer fiber (stops pagination mid-stream if needed)
    dispatcher.unsafeRunSync(producer.cancel)

    // 2. Drain any remaining queued items
    drainQueue()

    // 3. Close current batch + root
    if (currentBatch != null) currentBatch.close()
    if (currentRoot != null) currentRoot.close()

    // 4. Release HTTP client
    dispatcher.unsafeRunSync(releaseClient)

    // 5. Shut down dispatcher
    dispatcher.unsafeRunSync(releaseDispatcher)

    // 6. Close Arrow allocator (verifies all memory released)
    allocator.close()
  }

  private def drainQueue(): Unit = {
    var item = dispatcher.unsafeRunSync(queue.tryTake)
    while (item.isDefined) {
      item.flatten.foreach { case (batch, root) =>
        batch.close()
        root.close()
      }
      item = dispatcher.unsafeRunSync(queue.tryTake)
    }
  }

  /** Wrap Arrow VectorSchemaRoot as ColumnarBatch with zero-copy ArrowColumnVector wrappers. */
  protected def arrowToBatch(root: VectorSchemaRoot): ColumnarBatch = {
    val vectors = root.getFieldVectors.asScala.map { v =>
      new ArrowColumnVector(v)
    }.toArray
    val batch = new ColumnarBatch(vectors.asInstanceOf[Array[org.apache.spark.sql.vectorized.ColumnVector]])
    batch.setNumRows(root.getRowCount)
    batch
  }
}

package com.apilytics.spark

import cats.effect.{IO, Resource}
import com.apilytics.core.source.{ReadRequest, RecordPage, RecordSession, RecordSource}
import munit.FunSuite
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema => ArrowSchema}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.jdk.CollectionConverters._

/** Subclass fields must be set before anything the base class starts can use them (#262).
  *
  * The base class used to start its producer in its own constructor, which runs before a
  * subclass's `val`s are assigned. With a producer that touched `arrowSchema` on its own
  * thread, that was an intermittent NullPointerException. This reader touches the schema
  * while building its stream, which makes the same mistake deterministic: under the old
  * base class it fails every time, not occasionally.
  */
class LazyColumnarReaderInitSuite extends FunSuite {

  private object EmptySource extends RecordSource {
    override def session: Resource[IO, RecordSession] =
      Resource.pure(new RecordSession {
        override def pages(request: ReadRequest): fs2.Stream[IO, RecordPage] = fs2.Stream.empty
      })
  }

  private class SchemaTouchingReader extends LazyColumnarReader {
    override protected val allocator: RootAllocator = new RootAllocator()
    override protected val arrowSchema: ArrowSchema =
      new ArrowSchema(List(Field.nullable("id", new ArrowType.Int(32, true))).asJava)

    var fieldsSeen: Int = -1

    override protected def recordSource: RecordSource = EmptySource

    override protected def buildStream(
        session: RecordSession
    ): fs2.Stream[IO, (ColumnarBatch, VectorSchemaRoot)] = {
      fieldsSeen = arrowSchema.getFields.size() // NPE if the subclass isn't constructed yet
      fs2.Stream.empty
    }
  }

  test("the stream is built only after the subclass's fields are set") {
    val reader = new SchemaTouchingReader
    try {
      assert(!reader.next(), "an empty source yields no batches")
      assertEquals(reader.fieldsSeen, 1)
    } finally reader.close()
  }

  test("a reader closed before its first next() starts nothing and closes cleanly") {
    val reader = new SchemaTouchingReader
    reader.close()
    assertEquals(reader.fieldsSeen, -1, "closing an unstarted reader must not build its stream")
  }
}

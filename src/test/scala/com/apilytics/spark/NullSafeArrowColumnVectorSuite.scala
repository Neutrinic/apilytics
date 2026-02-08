package com.apilytics.spark

import munit.FunSuite
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.spark.sql.types._

class NullSafeArrowColumnVectorSuite extends FunSuite {

  private val allocator = new RootAllocator()

  override def afterAll(): Unit = {
    allocator.close()
    super.afterAll()
  }

  // ==========================================================================
  // Item 1: toSparkType throws on unknown vector types
  // ==========================================================================

  test("toSparkType throws on unsupported vector type") {
    // Use a vector type we don't support (e.g., ListVector)
    val listType = new ArrowType.List()
    val field = new Field("items", FieldType.nullable(listType), java.util.Collections.emptyList())
    val listVector = ListVector.empty("items", allocator)

    try {
      val ex = intercept[UnsupportedOperationException] {
        NullSafeArrowColumnVector.toSparkType(listVector)
      }
      assert(ex.getMessage.contains("Unsupported Arrow vector type"))
      assert(ex.getMessage.contains("ListVector"))
    } finally {
      listVector.close()
    }
  }

  test("toSparkType handles all supported types") {
    // Verify we correctly map all supported types
    val testCases = Seq(
      (() => new BitVector("bool", allocator), BooleanType),
      (() => new TinyIntVector("byte", allocator), ByteType),
      (() => new SmallIntVector("short", allocator), ShortType),
      (() => new IntVector("int", allocator), IntegerType),
      (() => new BigIntVector("long", allocator), LongType),
      (() => new Float4Vector("float", allocator), FloatType),
      (() => new Float8Vector("double", allocator), DoubleType),
      (() => new VarCharVector("string", allocator), StringType),
      (() => new VarBinaryVector("binary", allocator), BinaryType),
      (() => new DateDayVector("date", allocator), DateType)
    )

    for ((createVector, expectedType) <- testCases) {
      val vector = createVector()
      try {
        assertEquals(
          NullSafeArrowColumnVector.toSparkType(vector),
          expectedType,
          s"Failed for ${vector.getClass.getSimpleName}"
        )
      } finally {
        vector.close()
      }
    }
  }

  // ==========================================================================
  // Item 2: Timestamp unit conversion
  // ==========================================================================

  test("getLong converts timestamp seconds to microseconds") {
    val field = new Field(
      "ts",
      FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC")),
      null
    )
    val vector = new TimeStampSecTZVector("ts", allocator, "UTC")
    try {
      vector.allocateNew(2)
      vector.setSafe(0, 1000L) // 1000 seconds
      vector.setNull(1)
      vector.setValueCount(2)

      val col = new NullSafeArrowColumnVector(vector)
      assertEquals(col.getLong(0), 1000L * 1_000_000L) // should be microseconds
      assertEquals(col.getLong(1), 0L) // null returns 0
      assertEquals(col.isNullAt(1), true)
    } finally {
      vector.close()
    }
  }

  test("getLong converts timestamp milliseconds to microseconds") {
    val vector = new TimeStampMilliTZVector("ts", allocator, "UTC")
    try {
      vector.allocateNew(1)
      vector.setSafe(0, 1000L) // 1000 milliseconds
      vector.setValueCount(1)

      val col = new NullSafeArrowColumnVector(vector)
      assertEquals(col.getLong(0), 1000L * 1_000L) // should be microseconds
    } finally {
      vector.close()
    }
  }

  test("getLong preserves timestamp microseconds") {
    val vector = new TimeStampMicroTZVector("ts", allocator, "UTC")
    try {
      vector.allocateNew(1)
      vector.setSafe(0, 1_000_000L) // 1 million microseconds
      vector.setValueCount(1)

      val col = new NullSafeArrowColumnVector(vector)
      assertEquals(col.getLong(0), 1_000_000L) // unchanged
    } finally {
      vector.close()
    }
  }

  test("getLong converts timestamp nanoseconds to microseconds") {
    val vector = new TimeStampNanoTZVector("ts", allocator, "UTC")
    try {
      vector.allocateNew(1)
      vector.setSafe(0, 1_000_000_000L) // 1 billion nanoseconds
      vector.setValueCount(1)

      val col = new NullSafeArrowColumnVector(vector)
      assertEquals(col.getLong(0), 1_000_000L) // should be microseconds (divided by 1000)
    } finally {
      vector.close()
    }
  }

  // ==========================================================================
  // Item 3: Round-trip null semantics
  // ==========================================================================

  test("null values are correctly identified across all types") {
    // Test that isNullAt() works correctly and getters return safe defaults
    val intVector = new IntVector("int", allocator)
    val stringVector = new VarCharVector("string", allocator)
    val doubleVector = new Float8Vector("double", allocator)
    val boolVector = new BitVector("bool", allocator)

    try {
      // Set up vectors with null at index 0, value at index 1
      intVector.allocateNew(2)
      intVector.setNull(0)
      intVector.setSafe(1, 42)
      intVector.setValueCount(2)

      stringVector.allocateNew(2)
      stringVector.setNull(0)
      stringVector.setSafe(1, "hello".getBytes)
      stringVector.setValueCount(2)

      doubleVector.allocateNew(2)
      doubleVector.setNull(0)
      doubleVector.setSafe(1, 3.14)
      doubleVector.setValueCount(2)

      boolVector.allocateNew(2)
      boolVector.setNull(0)
      boolVector.setSafe(1, 1)
      boolVector.setValueCount(2)

      // Wrap and verify null handling
      val intCol = new NullSafeArrowColumnVector(intVector)
      assertEquals(intCol.isNullAt(0), true)
      assertEquals(intCol.isNullAt(1), false)
      assertEquals(intCol.getInt(0), 0) // safe default
      assertEquals(intCol.getInt(1), 42)

      val stringCol = new NullSafeArrowColumnVector(stringVector)
      assertEquals(stringCol.isNullAt(0), true)
      assertEquals(stringCol.isNullAt(1), false)
      assertEquals(stringCol.getUTF8String(0).toString, "") // empty, not null
      assertEquals(stringCol.getUTF8String(1).toString, "hello")

      val doubleCol = new NullSafeArrowColumnVector(doubleVector)
      assertEquals(doubleCol.isNullAt(0), true)
      assertEquals(doubleCol.isNullAt(1), false)
      assertEquals(doubleCol.getDouble(0), 0.0)
      assertEquals(doubleCol.getDouble(1), 3.14)

      val boolCol = new NullSafeArrowColumnVector(boolVector)
      assertEquals(boolCol.isNullAt(0), true)
      assertEquals(boolCol.isNullAt(1), false)
      assertEquals(boolCol.getBoolean(0), false) // safe default
      assertEquals(boolCol.getBoolean(1), true)
    } finally {
      intVector.close()
      stringVector.close()
      doubleVector.close()
      boolVector.close()
    }
  }

  test("hasNull and numNulls report correctly") {
    val vector = new IntVector("int", allocator)
    try {
      vector.allocateNew(5)
      vector.setSafe(0, 1)
      vector.setNull(1)
      vector.setSafe(2, 3)
      vector.setNull(3)
      vector.setNull(4)
      vector.setValueCount(5)

      val col = new NullSafeArrowColumnVector(vector)
      assertEquals(col.hasNull, true)
      assertEquals(col.numNulls(), 3)
    } finally {
      vector.close()
    }
  }

  test("all-null column is handled correctly") {
    val vector = new VarCharVector("string", allocator)
    try {
      vector.allocateNew(3)
      vector.setNull(0)
      vector.setNull(1)
      vector.setNull(2)
      vector.setValueCount(3)

      val col = new NullSafeArrowColumnVector(vector)
      assertEquals(col.hasNull, true)
      assertEquals(col.numNulls(), 3)

      // All getters should return safe defaults without throwing
      for (i <- 0 until 3) {
        assertEquals(col.isNullAt(i), true)
        assertEquals(col.getUTF8String(i).toString, "")
      }
    } finally {
      vector.close()
    }
  }

  test("no-null column reports hasNull=false") {
    val vector = new IntVector("int", allocator)
    try {
      vector.allocateNew(2)
      vector.setSafe(0, 10)
      vector.setSafe(1, 20)
      vector.setValueCount(2)

      val col = new NullSafeArrowColumnVector(vector)
      assertEquals(col.hasNull, false)
      assertEquals(col.numNulls(), 0)
    } finally {
      vector.close()
    }
  }
}

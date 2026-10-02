package com.apilytics.core.arrow

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

/** What the converter did to values that weren't already in their column's type (#308, #309).
  *
  * A value with exactly one reading is converted: `"42"` into an integer column. A value that
  * doesn't fit or doesn't parse becomes NULL rather than failing the read. Both are counted, and
  * the first NULL in each column is reported through `warn`, naming the column and the value.
  *
  * One instance per reader, so per task. The converter runs on the reader's producer fiber and
  * Spark reads the counts from the task thread, hence the concurrent types.
  */
final class ConversionStats(warn: String => Unit) {
  private val convertedCount = new LongAdder
  private val nulledCount    = new LongAdder
  private val warned         = ConcurrentHashMap.newKeySet[String]()

  def converted: Long = convertedCount.sum()
  def nulled: Long    = nulledCount.sum()

  private[arrow] def recordConverted(): Unit = convertedCount.increment()

  private[arrow] def recordNulled(column: String, value: io.circe.Json, columnType: String): Unit = {
    nulledCount.increment()
    if (warned.add(column)) {
      val shown = value.noSpaces
      val example = if (shown.length > 100) shown.take(100) + "..." else shown
      warn(
        s"Column '$column' ($columnType): $example doesn't fit or doesn't parse, so it was " +
          "read as NULL. Further values in this column are counted in the 'values replaced " +
          "with NULL' metric, not logged."
      )
    }
  }
}

object ConversionStats {

  /** For callers with nowhere to report, such as tests and the driver's sample reads. */
  def ignore: ConversionStats = new ConversionStats(_ => ())
}

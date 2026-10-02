package com.apilytics.spark

import com.apilytics.core.arrow.ConversionStats
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric, CustomTaskMetric}

/** What the converter did to values not already in their column's type, shown in the Spark
  * UI's SQL tab next to the scan (#308, #309).
  *
  * "values converted" counts values with one reading in the column's type, such as `"42"` in
  * an integer column. "values replaced with NULL" counts values that didn't fit or didn't
  * parse. Every columnar scan declares both, and every columnar reader reports them.
  */
object ConversionMetrics {
  val ConvertedName = "valuesConverted"
  val NulledName    = "valuesNulled"

  def supported: Array[CustomMetric] = Array(new ValuesConvertedMetric, new ValuesNulledMetric)

  def taskValues(stats: ConversionStats): Array[CustomTaskMetric] =
    Array(taskMetric(ConvertedName, stats.converted), taskMetric(NulledName, stats.nulled))

  private def taskMetric(metricName: String, metricValue: Long): CustomTaskMetric =
    new CustomTaskMetric {
      override def name(): String = metricName
      override def value(): Long  = metricValue
    }
}

// Spark instantiates these by class name on the driver, so they need no-argument constructors.

class ValuesConvertedMetric extends CustomSumMetric {
  override def name(): String        = ConversionMetrics.ConvertedName
  override def description(): String = "values converted"
}

class ValuesNulledMetric extends CustomSumMetric {
  override def name(): String        = ConversionMetrics.NulledName
  override def description(): String = "values replaced with NULL"
}

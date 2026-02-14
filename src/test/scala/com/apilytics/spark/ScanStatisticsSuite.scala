package com.apilytics.spark

import com.apilytics.core.config.{PaginationConfig, PaginationStyle}
import munit.FunSuite
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class ScanStatisticsSuite extends FunSuite {

  private val testSchema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("email", StringType)
  ))

  private val defaultPagination = PaginationConfig(
    style = PaginationStyle.Offset,
    maxPageSize = 100,
    maxPages = 1000
  )

  test("estimate uses pushed limit when available") {
    val stats = ScanStatistics.estimate(Some(50), defaultPagination, testSchema)

    assertEquals(stats.numRows().getAsLong, 50L)
    // 3 fields * 50 bytes each * 50 rows = 7500 bytes
    assertEquals(stats.sizeInBytes().getAsLong, 7500L)
  }

  test("estimate returns empty stats when no limit (unknown size)") {
    val stats = ScanStatistics.estimate(None, defaultPagination, testSchema)

    // Without LIMIT, we don't know the size - return empty to avoid bad broadcast decisions
    assert(!stats.numRows().isPresent)
    assert(!stats.sizeInBytes().isPresent)
  }

  test("estimate with larger schema") {
    val largeSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("email", StringType),
      StructField("address", StringType),
      StructField("phone", StringType),
      StructField("city", StringType),
      StructField("country", StringType),
      StructField("zip", StringType),
      StructField("created_at", StringType),
      StructField("updated_at", StringType)
    ))
    val stats = ScanStatistics.estimate(Some(10), defaultPagination, largeSchema)

    assertEquals(stats.numRows().getAsLong, 10L)
    // 10 fields * 50 bytes * 10 rows = 5000 bytes
    assertEquals(stats.sizeInBytes().getAsLong, 5000L)
  }

  test("estimate with LIMIT 1") {
    val stats = ScanStatistics.estimate(Some(1), defaultPagination, testSchema)

    assertEquals(stats.numRows().getAsLong, 1L)
    // 3 fields * 50 bytes * 1 row = 150 bytes
    assertEquals(stats.sizeInBytes().getAsLong, 150L)
  }

  test("statistics values are present with limit") {
    val stats = ScanStatistics.estimate(Some(100), defaultPagination, testSchema)

    assert(stats.numRows().isPresent)
    assert(stats.sizeInBytes().isPresent)
  }

  test("statistics values are empty without limit") {
    val stats = ScanStatistics.estimate(None, defaultPagination, testSchema)

    assert(!stats.numRows().isPresent)
    assert(!stats.sizeInBytes().isPresent)
  }
}

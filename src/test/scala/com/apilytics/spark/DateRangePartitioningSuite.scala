package com.apilytics.spark

import com.apilytics.core.config._
import com.apilytics.core.openapi.Endpoint
import com.apilytics.core.schema.SourceSchema
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema => ArrowSchema}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class DateRangePartitioningSuite extends FunSuite {

  private val dummySchema = new ArrowSchema(List(
    new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
  ).asJava)

  private val dummyEndpoint = Endpoint(
    path = "/events",
    operationId = Some("getEvents"),
    responseSchema = SourceSchema.ObjectType(Map("id" -> SourceSchema.IntegerType())),
    queryParams = Nil
  )

  private val dummySourceConfig = SourceConfig(
    openapi = "test.yaml",
    auth = AuthConfig(authType = AuthType.Bearer, token = Some("test")),
    http = HttpConfig(maxBackoff = 30.seconds, timeout = 30.seconds)
  )

  test("Loader parses partition config") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    partition {
        |      column = "created_at"
        |      range = "1 day"
        |      start-param = "created_after"
        |      end-param = "created_before"
        |    }
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    val partition = result.tables("events").partition.get.asInstanceOf[PartitionConfig.DateRange]

    assertEquals(partition.column, "created_at")
    assertEquals(partition.range, 1.day)
    assertEquals(partition.startParam, "created_after")
    assertEquals(partition.endParam, "created_before")
    assertEquals(partition.format, "yyyy-MM-dd'T'HH:mm:ss'Z'")
  }

  test("Loader parses partition config with custom format") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    partition {
        |      column = "date"
        |      range = "7 days"
        |      start-param = "since"
        |      end-param = "until"
        |      format = "yyyy-MM-dd"
        |    }
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    val partition = result.tables("events").partition.get.asInstanceOf[PartitionConfig.DateRange]

    assertEquals(partition.range, 7.days)
    assertEquals(partition.format, "yyyy-MM-dd")
  }

  test("RESTScan creates single partition when no partition config") {
    val tableConfig = TableConfig(endpoint = "/events")
    val table = new RESTTable(
      tableName = "events",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = dummySourceConfig,
      baseUrl = "https://api.example.com"
    )

    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map.empty,
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 1)
  }

  test("RESTScan creates single partition when date range not in query") {
    val partitionConfig = PartitionConfig.DateRange(
      column = "created_at",
      range = 1.day,
      startParam = "created_after",
      endParam = "created_before"
    )
    val tableConfig = TableConfig(endpoint = "/events", partition = Some(partitionConfig))
    val table = new RESTTable(
      tableName = "events",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = dummySourceConfig,
      baseUrl = "https://api.example.com"
    )

    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map("other_param" -> "value"),
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 1)
  }

  test("RESTScan creates multiple partitions for date range query") {
    val partitionConfig = PartitionConfig.DateRange(
      column = "created_at",
      range = 1.day,
      startParam = "created_after",
      endParam = "created_before"
    )
    val tableConfig = TableConfig(endpoint = "/events", partition = Some(partitionConfig))
    val table = new RESTTable(
      tableName = "events",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = dummySourceConfig,
      baseUrl = "https://api.example.com"
    )

    // Query for 3 days should create 3 partitions
    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map(
        "created_after" -> "2024-01-01T00:00:00Z",
        "created_before" -> "2024-01-04T00:00:00Z"
      ),
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 3)

    // Verify each partition has correct date range
    val restPartitions = partitions.map(_.asInstanceOf[RESTInputPartition])

    assertEquals(restPartitions(0).pushedParams("created_after"), "2024-01-01T00:00:00Z")
    assertEquals(restPartitions(0).pushedParams("created_before"), "2024-01-02T00:00:00Z")

    assertEquals(restPartitions(1).pushedParams("created_after"), "2024-01-02T00:00:00Z")
    assertEquals(restPartitions(1).pushedParams("created_before"), "2024-01-03T00:00:00Z")

    assertEquals(restPartitions(2).pushedParams("created_after"), "2024-01-03T00:00:00Z")
    assertEquals(restPartitions(2).pushedParams("created_before"), "2024-01-04T00:00:00Z")
  }

  test("RESTScan handles partial day range correctly") {
    val partitionConfig = PartitionConfig.DateRange(
      column = "created_at",
      range = 1.day,
      startParam = "start",
      endParam = "end"
    )
    val tableConfig = TableConfig(endpoint = "/events", partition = Some(partitionConfig))
    val table = new RESTTable(
      tableName = "events",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = dummySourceConfig,
      baseUrl = "https://api.example.com"
    )

    // Query for 1.5 days should create 2 partitions
    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map(
        "start" -> "2024-01-01T00:00:00Z",
        "end" -> "2024-01-02T12:00:00Z"
      ),
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 2)

    val restPartitions = partitions.map(_.asInstanceOf[RESTInputPartition])
    // First partition: full day
    assertEquals(restPartitions(0).pushedParams("start"), "2024-01-01T00:00:00Z")
    assertEquals(restPartitions(0).pushedParams("end"), "2024-01-02T00:00:00Z")
    // Second partition: remaining 12 hours
    assertEquals(restPartitions(1).pushedParams("start"), "2024-01-02T00:00:00Z")
    assertEquals(restPartitions(1).pushedParams("end"), "2024-01-02T12:00:00Z")
  }

  test("RESTScan preserves other pushed params in partitions") {
    val partitionConfig = PartitionConfig.DateRange(
      column = "created_at",
      range = 1.day,
      startParam = "created_after",
      endParam = "created_before"
    )
    val tableConfig = TableConfig(endpoint = "/events", partition = Some(partitionConfig))
    val table = new RESTTable(
      tableName = "events",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = dummySourceConfig,
      baseUrl = "https://api.example.com"
    )

    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map(
        "created_after" -> "2024-01-01T00:00:00Z",
        "created_before" -> "2024-01-02T00:00:00Z",
        "status" -> "active"
      ),
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 1)

    val restPartition = partitions.head.asInstanceOf[RESTInputPartition]
    assertEquals(restPartition.pushedParams("status"), "active")
  }

  test("RESTScan handles hourly partitions") {
    val partitionConfig = PartitionConfig.DateRange(
      column = "timestamp",
      range = 1.hour,
      startParam = "from",
      endParam = "to"
    )
    val tableConfig = TableConfig(endpoint = "/logs", partition = Some(partitionConfig))
    val table = new RESTTable(
      tableName = "logs",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = dummySourceConfig,
      baseUrl = "https://api.example.com"
    )

    // Query for 3 hours should create 3 partitions
    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map(
        "from" -> "2024-01-01T00:00:00Z",
        "to" -> "2024-01-01T03:00:00Z"
      ),
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 3)
  }

  test("RESTScan handles empty range (start == end)") {
    val partitionConfig = PartitionConfig.DateRange(
      column = "created_at",
      range = 1.day,
      startParam = "start",
      endParam = "end"
    )
    val tableConfig = TableConfig(endpoint = "/events", partition = Some(partitionConfig))
    val table = new RESTTable(
      tableName = "events",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = dummySourceConfig,
      baseUrl = "https://api.example.com"
    )

    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map(
        "start" -> "2024-01-01T00:00:00Z",
        "end" -> "2024-01-01T00:00:00Z"
      ),
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 0)
  }

  test("RESTScan handles range larger than span (single partition)") {
    val partitionConfig = PartitionConfig.DateRange(
      column = "created_at",
      range = 30.days,
      startParam = "start",
      endParam = "end"
    )
    val tableConfig = TableConfig(endpoint = "/events", partition = Some(partitionConfig))
    val table = new RESTTable(
      tableName = "events",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = dummySourceConfig,
      baseUrl = "https://api.example.com"
    )

    // 3-day range with 30-day partition size = 1 partition
    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map(
        "start" -> "2024-01-01T00:00:00Z",
        "end" -> "2024-01-04T00:00:00Z"
      ),
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 1)

    val restPartition = partitions.head.asInstanceOf[RESTInputPartition]
    assertEquals(restPartition.pushedParams("start"), "2024-01-01T00:00:00Z")
    assertEquals(restPartition.pushedParams("end"), "2024-01-04T00:00:00Z")
  }

  test("RESTScan handles malformed date strings gracefully") {
    val partitionConfig = PartitionConfig.DateRange(
      column = "created_at",
      range = 1.day,
      startParam = "start",
      endParam = "end"
    )
    val tableConfig = TableConfig(endpoint = "/events", partition = Some(partitionConfig))
    val table = new RESTTable(
      tableName = "events",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = dummySourceConfig,
      baseUrl = "https://api.example.com"
    )

    // Invalid date format should fall back to single partition
    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map(
        "start" -> "not-a-date",
        "end" -> "2024-01-04T00:00:00Z"
      ),
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 1)

    // Original params should be preserved in fallback
    val restPartition = partitions.head.asInstanceOf[RESTInputPartition]
    assertEquals(restPartition.pushedParams("start"), "not-a-date")
  }

  test("Loader parses table without partition config") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    assertEquals(result.tables("events").partition, None)
  }

  test("Loader rejects infinite duration for partition range") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    partition {
        |      column = "created_at"
        |      range = "Inf"
        |      start-param = "start"
        |      end-param = "end"
        |    }
        |  }
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      Loader.load(config)
    }
    assert(ex.getMessage.contains("finite"))
  }

  // ==================== Enum Partitioning Tests ====================

  test("Loader parses enum partition config") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  orders {
        |    endpoint = "/orders"
        |    partition {
        |      type = "enum"
        |      param = "status"
        |      values = ["pending", "processing", "shipped", "delivered"]
        |    }
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    val partition = result.tables("orders").partition.get.asInstanceOf[PartitionConfig.Enum]

    assertEquals(partition.param, "status")
    assertEquals(partition.values, List("pending", "processing", "shipped", "delivered"))
  }

  test("RESTScan creates N partitions for N enum values") {
    val partitionConfig = PartitionConfig.Enum(
      param = "status",
      values = List("pending", "processing", "shipped")
    )
    val tableConfig = TableConfig(endpoint = "/orders", partition = Some(partitionConfig))
    val table = new RESTTable(
      tableName = "orders",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = dummySourceConfig,
      baseUrl = "https://api.example.com"
    )

    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map.empty,
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 3)
  }

  test("Enum partition sets correct param value for each partition") {
    val partitionConfig = PartitionConfig.Enum(
      param = "status",
      values = List("active", "inactive")
    )
    val tableConfig = TableConfig(endpoint = "/users", partition = Some(partitionConfig))
    val table = new RESTTable(
      tableName = "users",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = dummySourceConfig,
      baseUrl = "https://api.example.com"
    )

    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map("other" -> "value"),
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    val restPartitions = partitions.map(_.asInstanceOf[RESTInputPartition])

    assertEquals(restPartitions(0).pushedParams("status"), "active")
    assertEquals(restPartitions(0).pushedParams("other"), "value")

    assertEquals(restPartitions(1).pushedParams("status"), "inactive")
    assertEquals(restPartitions(1).pushedParams("other"), "value")
  }

  test("Enum partition distributes rate limit across partitions") {
    val partitionConfig = PartitionConfig.Enum(
      param = "status",
      values = List("a", "b", "c", "d")
    )
    val tableConfig = TableConfig(endpoint = "/items", partition = Some(partitionConfig))
    val sourceConfig = dummySourceConfig.copy(
      http = HttpConfig(maxBackoff = 30.seconds, timeout = 30.seconds, rateLimit = Some(100))
    )
    val table = new RESTTable(
      tableName = "items",
      arrowSchema = dummySchema,
      endpoint = dummyEndpoint,
      tableConfig = Some(tableConfig),
      sourceConfig = sourceConfig,
      baseUrl = "https://api.example.com"
    )

    val scan = new RESTScan(
      table = table,
      arrowSchema = dummySchema,
      prunedSchema = None,
      pushedParams = Map.empty,
      pushedLimit = None
    )

    val partitions = scan.planInputPartitions()
    val restPartitions = partitions.map(_.asInstanceOf[RESTInputPartition])

    // 100 rps / 4 partitions = 25 rps each
    restPartitions.foreach { p =>
      assertEquals(p.effectiveRateLimit, Some(25))
    }
  }

  test("Loader validates enum partition requires param") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  orders {
        |    endpoint = "/orders"
        |    partition {
        |      type = "enum"
        |      values = ["pending", "shipped"]
        |    }
        |  }
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      Loader.load(config)
    }
    assert(ex.getMessage.contains("param"))
  }

  test("Loader validates enum partition requires non-empty values") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  orders {
        |    endpoint = "/orders"
        |    partition {
        |      type = "enum"
        |      param = "status"
        |      values = []
        |    }
        |  }
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      Loader.load(config)
    }
    assert(ex.getMessage.contains("non-empty"))
  }

  test("Loader rejects unknown partition type") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    partition {
        |      type = "unknown"
        |      param = "foo"
        |    }
        |  }
        |}
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      Loader.load(config)
    }
    assert(ex.getMessage.contains("Unknown partition type"))
  }

  test("Explicit date-range type works") {
    val config = ConfigFactory.parseString(
      """
        |openapi = "spec.json"
        |auth { type = bearer, token = "t" }
        |tables {
        |  events {
        |    endpoint = "/events"
        |    partition {
        |      type = "date-range"
        |      column = "created_at"
        |      range = "1 day"
        |      start-param = "start"
        |      end-param = "end"
        |    }
        |  }
        |}
        |""".stripMargin)

    val result = Loader.load(config)
    val partition = result.tables("events").partition.get.asInstanceOf[PartitionConfig.DateRange]

    assertEquals(partition.column, "created_at")
  }
}

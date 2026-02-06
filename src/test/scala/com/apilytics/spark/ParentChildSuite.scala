package com.apilytics.spark

import com.apilytics.core.config._
import com.apilytics.core.openapi.{Endpoint, OpenAPISchema, QueryParam}
import com.apilytics.core.schema.SchemaMapper
import munit.FunSuite
import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ParentChildSuite extends FunSuite {

  private val defaultHttp = HttpConfig(maxRetries = 0, maxBackoff = 1.second, timeout = 30.seconds)
  private val defaultAuth = AuthConfig(authType = AuthType.Bearer, token = Some("test"))

  // --- Config and Loader tests ---

  test("TableConfig parses parent-table and parent-key fields") {
    val config = TableConfig(
      endpoint = "/customers/{customer_id}/orders",
      parentTable = Some("customers"),
      parentKey = Some("id"),
      joinStrategy = Some(JoinStrategy.NestedLoop)
    )

    assertEquals(config.parentTable, Some("customers"))
    assertEquals(config.parentKey, Some("id"))
    assertEquals(config.joinStrategy, Some(JoinStrategy.NestedLoop))
  }

  test("Loader parses parent-child table config from HOCON") {
    import com.typesafe.config.ConfigFactory

    val hocon = """
      openapi = "test.yaml"
      auth { type = "bearer", token = "test" }
      http { timeout = "30s", max-backoff = "30s" }
      tables {
        customer_orders {
          endpoint = "/customers/{customer_id}/orders"
          parent-table = "customers"
          parent-key = "id"
          join-strategy = "nested_loop"
        }
      }
    """

    val config = Loader.load(ConfigFactory.parseString(hocon))
    val tableConfig = config.tables("customer_orders")

    assertEquals(tableConfig.endpoint, "/customers/{customer_id}/orders")
    assertEquals(tableConfig.parentTable, Some("customers"))
    assertEquals(tableConfig.parentKey, Some("id"))
    assertEquals(tableConfig.joinStrategy, Some(JoinStrategy.NestedLoop))
  }

  test("Loader handles table without parent-child config") {
    import com.typesafe.config.ConfigFactory

    val hocon = """
      openapi = "test.yaml"
      auth { type = "bearer", token = "test" }
      http { timeout = "30s", max-backoff = "30s" }
      tables {
        simple {
          endpoint = "/items"
        }
      }
    """

    val config = Loader.load(ConfigFactory.parseString(hocon))
    val tableConfig = config.tables("simple")

    assertEquals(tableConfig.parentTable, None)
    assertEquals(tableConfig.parentKey, None)
    assertEquals(tableConfig.joinStrategy, None)
  }

  // --- ParentChildTable tests ---

  test("ParentChildTable extracts path parameter name from endpoint template") {
    val parentSchema = OpenAPISchema.ObjectType(
      Map("id" -> OpenAPISchema.IntegerType(), "name" -> OpenAPISchema.StringType())
    )
    val parentEndpoint = Endpoint(
      path = "/customers",
      operationId = Some("listCustomers"),
      responseSchema = parentSchema,
      queryParams = Nil
    )

    val tableConfig = TableConfig(
      endpoint = "/customers/{customer_id}/orders",
      parentTable = Some("customers"),
      parentKey = Some("id"),
      joinStrategy = Some(JoinStrategy.NestedLoop)
    )

    val sourceConfig = SourceConfig(
      openapi = "test.yaml",
      auth = defaultAuth,
      http = defaultHttp
    )

    val table = new ParentChildTable(
      tableName = "customer_orders",
      childEndpointTemplate = "/customers/{customer_id}/orders",
      parentTableName = "customers",
      parentKey = "id",
      parentEndpoint = parentEndpoint,
      childResponseSchema = parentSchema, // simplified
      tableConfig = tableConfig,
      sourceConfig = sourceConfig,
      baseUrl = "http://localhost"
    )

    assertEquals(table.pathParamName, "customer_id")
    assertEquals(table.parentKeyColumn, "_parent_customer_id")
  }

  test("ParentChildTable throws if endpoint has no path parameter") {
    val parentSchema = OpenAPISchema.ObjectType(
      Map("id" -> OpenAPISchema.IntegerType())
    )
    val parentEndpoint = Endpoint(
      path = "/customers",
      operationId = Some("listCustomers"),
      responseSchema = parentSchema,
      queryParams = Nil
    )

    val tableConfig = TableConfig(
      endpoint = "/orders", // no path parameter!
      parentTable = Some("customers"),
      parentKey = Some("id"),
      joinStrategy = Some(JoinStrategy.NestedLoop)
    )

    val sourceConfig = SourceConfig(
      openapi = "test.yaml",
      auth = defaultAuth,
      http = defaultHttp
    )

    intercept[IllegalArgumentException] {
      new ParentChildTable(
        tableName = "customer_orders",
        childEndpointTemplate = "/orders",
        parentTableName = "customers",
        parentKey = "id",
        parentEndpoint = parentEndpoint,
        childResponseSchema = parentSchema,
        tableConfig = tableConfig,
        sourceConfig = sourceConfig,
        baseUrl = "http://localhost"
      )
    }
  }

  test("ParentChildTable schema includes parent key column") {
    val parentSchema = OpenAPISchema.ObjectType(
      Map("id" -> OpenAPISchema.IntegerType(), "name" -> OpenAPISchema.StringType())
    )
    val childSchema = OpenAPISchema.ObjectType(
      Map("order_id" -> OpenAPISchema.IntegerType(), "total" -> OpenAPISchema.NumberType())
    )
    val parentEndpoint = Endpoint(
      path = "/customers",
      operationId = Some("listCustomers"),
      responseSchema = parentSchema,
      queryParams = Nil
    )

    val tableConfig = TableConfig(
      endpoint = "/customers/{customer_id}/orders",
      parentTable = Some("customers"),
      parentKey = Some("id"),
      joinStrategy = Some(JoinStrategy.NestedLoop)
    )

    val sourceConfig = SourceConfig(
      openapi = "test.yaml",
      auth = defaultAuth,
      http = defaultHttp
    )

    val table = new ParentChildTable(
      tableName = "customer_orders",
      childEndpointTemplate = "/customers/{customer_id}/orders",
      parentTableName = "customers",
      parentKey = "id",
      parentEndpoint = parentEndpoint,
      childResponseSchema = childSchema,
      tableConfig = tableConfig,
      sourceConfig = sourceConfig,
      baseUrl = "http://localhost"
    )

    val schema = table.schema()
    val fieldNames = schema.fieldNames.toSet

    // Should have parent key column + child fields
    assert(fieldNames.contains("_parent_customer_id"))
    assert(fieldNames.contains("order_id"))
    assert(fieldNames.contains("total"))
  }

  // --- ScanBuilder tests ---

  private def mkBuilder(filters: List[FilterConfig] = Nil): ParentChildScanBuilder = {
    val parentSchema = OpenAPISchema.ObjectType(
      Map("id" -> OpenAPISchema.IntegerType(), "name" -> OpenAPISchema.StringType())
    )
    val childSchema = OpenAPISchema.ObjectType(
      Map("order_id" -> OpenAPISchema.IntegerType(), "status" -> OpenAPISchema.StringType())
    )
    val parentEndpoint = Endpoint(
      path = "/customers",
      operationId = Some("listCustomers"),
      responseSchema = parentSchema,
      queryParams = Nil
    )

    val tableConfig = TableConfig(
      endpoint = "/customers/{customer_id}/orders",
      parentTable = Some("customers"),
      parentKey = Some("id"),
      joinStrategy = Some(JoinStrategy.NestedLoop),
      filters = filters
    )

    val sourceConfig = SourceConfig(
      openapi = "test.yaml",
      auth = defaultAuth,
      http = defaultHttp
    )

    val table = new ParentChildTable(
      tableName = "customer_orders",
      childEndpointTemplate = "/customers/{customer_id}/orders",
      parentTableName = "customers",
      parentKey = "id",
      parentEndpoint = parentEndpoint,
      childResponseSchema = childSchema,
      tableConfig = tableConfig,
      sourceConfig = sourceConfig,
      baseUrl = "http://localhost"
    )

    table.newScanBuilder(new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))
      .asInstanceOf[ParentChildScanBuilder]
  }

  private def mkPredicate(op: String, column: String, value: String): Predicate = {
    val ref = Expressions.column(column)
    val lit = Expressions.literal(UTF8String.fromString(value))
    new Predicate(op, Array[Expression](ref, lit))
  }

  test("ParentChildScanBuilder supports filter pushdown") {
    val builder = mkBuilder(List(
      FilterConfig(param = "status", column = "status", operators = List("eq"))
    ))

    val predicate = mkPredicate("=", "status", "shipped")
    val remaining = builder.pushPredicates(Array(predicate))

    assertEquals(remaining.length, 0)
    assertEquals(builder.pushedPredicates().length, 1)
  }

  test("ParentChildScanBuilder supports limit pushdown") {
    val builder = mkBuilder()

    val consumed = builder.pushLimit(10)
    assertEquals(consumed, false) // Spark still enforces post-scan
  }

  test("ParentChildScanBuilder wires pushed params and limit to InputPartition") {
    val builder = mkBuilder(List(
      FilterConfig(param = "status", column = "status", operators = List("eq"))
    ))

    builder.pushPredicates(Array(mkPredicate("=", "status", "pending")))
    builder.pushLimit(25)

    val scan = builder.build().asInstanceOf[ParentChildScan]
    val partitions = scan.planInputPartitions()
    assertEquals(partitions.length, 1)

    val partition = partitions(0).asInstanceOf[ParentChildInputPartition]
    assertEquals(partition.pushedParams, Map("status" -> "pending"))
    assertEquals(partition.pushedLimit, Some(25))
  }

  test("ParentChildPartitionReaderFactory supports columnar reads") {
    val factory = new ParentChildPartitionReaderFactory()
    val parentSchema = OpenAPISchema.ObjectType(Map("id" -> OpenAPISchema.IntegerType()))
    val partition = ParentChildInputPartition(
      childEndpointTemplate = "/customers/{customer_id}/orders",
      parentEndpoint = Endpoint(
        path = "/customers",
        operationId = None,
        responseSchema = parentSchema,
        queryParams = Nil
      ),
      childResponseSchema = parentSchema,
      parentKey = "id",
      pathParamName = "customer_id",
      parentKeyColumn = "_parent_customer_id",
      tableConfig = TableConfig(endpoint = "/customers/{customer_id}/orders"),
      sourceConfig = SourceConfig(
        openapi = "test.yaml",
        auth = defaultAuth,
        http = defaultHttp
      ),
      baseUrl = "http://localhost",
      arrowSchemaJson = "{}"
    )
    assert(factory.supportColumnarReads(partition))
  }

}

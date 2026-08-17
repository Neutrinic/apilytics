package com.apilytics.spark

import com.apilytics.core.config.{SchemaMode, SourceConfig, TableConfig}
import com.apilytics.core.source.SourceHandle
import com.apilytics.core.schema.SourceSchema
import com.apilytics.core.schema.SchemaMapper
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._

/** A table that fetches child data based on parent table keys.
  *
  * For each row in the parent table, substitutes the parent key into the child endpoint
  * path template and fetches the child records. Results include a parent key column
  * to enable joining.
  *
  * Example: For endpoint `/customers/{customer_id}/orders` with parent table `customers`
  * and parent key `id`, this table will:
  * 1. Fetch all customers
  * 2. For each customer, substitute `id` into the path → `/customers/123/orders`
  * 3. Fetch orders for each customer
  * 4. Include `_parent_customer_id` column in results for joining
  */
class ParentChildTable(
    val tableName: String,
    val childEndpointTemplate: String,
    val parentTableName: String,
    val parentKey: String,
    val parentHandle: SourceHandle,
    val childResponseSchema: SourceSchema.ObjectType,
    val tableConfig: TableConfig,
    val sourceConfig: SourceConfig,
    val baseUrl: String
) extends Table with SupportsRead {

  // Path parameter name extracted from endpoint template (e.g., "customer_id" from "/customers/{customer_id}/orders")
  val pathParamName: String = extractPathParam(childEndpointTemplate)
    .getOrElse(throw new IllegalArgumentException(
      s"Child endpoint '$childEndpointTemplate' must contain a path parameter like {param_name}"
    ))

  // Parent key column name in output (e.g., "_parent_customer_id")
  val parentKeyColumn: String = s"_parent_$pathParamName"

  private lazy val (arrowSchema, sparkSchema) = buildSchema()

  /** Build schema: parent key column + child response fields. */
  private def buildSchema(): (ArrowSchema, StructType) = {
    // Child fields from response schema
    val childSchema = SchemaMapper.toArrowSchemaWithMode(
      childResponseSchema,
      sourceConfig.schema.flattenDepth,
      sourceConfig.schema.mode
    )

    // Add parent key column (string type) at the beginning
    val parentKeyField = new org.apache.arrow.vector.types.pojo.Field(
      parentKeyColumn,
      org.apache.arrow.vector.types.pojo.FieldType.nullable(
        new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()
      ),
      java.util.Collections.emptyList()
    )

    val combinedFields = new java.util.ArrayList[org.apache.arrow.vector.types.pojo.Field]()
    combinedFields.add(parentKeyField)
    combinedFields.addAll(childSchema.getFields)

    val arrow = new ArrowSchema(combinedFields)
    val spark = ArrowSchemaConverter.toSparkSchema(arrow)
    (arrow, spark)
  }

  override def name(): String = tableName

  override def schema(): StructType = sparkSchema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new ParentChildScanBuilder(this, arrowSchema)

  /** Extract the first path parameter name from endpoint template.
    * E.g., "/customers/{customer_id}/orders" → Some("customer_id")
    */
  private def extractPathParam(template: String): Option[String] = {
    val paramPattern = """\{([^}]+)\}""".r
    paramPattern.findFirstMatchIn(template).map(_.group(1))
  }
}

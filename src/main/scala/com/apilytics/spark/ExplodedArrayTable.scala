package com.apilytics.spark

import com.apilytics.core.config.{SchemaMode, SourceConfig, TableConfig}
import org.slf4j.LoggerFactory
import com.apilytics.core.openapi.{Endpoint, OpenAPISchema}
import com.apilytics.core.schema.SchemaMapper
import com.apilytics.core.schema.SchemaMapper.ArrayFieldInfo
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._

/** A virtual table that explodes an array field from a base table.
  * Each row contains all parent scalar fields plus one array element.
  */
class ExplodedArrayTable(
    val tableName: String,
    val baseTableName: String,
    val arrayFieldName: String,
    val arrayFieldInfo: ArrayFieldInfo,
    val endpoint: Endpoint,
    val tableConfig: Option[TableConfig],
    val sourceConfig: SourceConfig,
    val baseUrl: String
) extends Table with SupportsRead {

  private val log = LoggerFactory.getLogger(getClass)

  // Build schema: parent scalar fields + exploded array element field
  private lazy val (arrowSchema, sparkSchema) = buildExplodedSchema()

  private def buildExplodedSchema(): (ArrowSchema, StructType) = {
    // Exploded views require schema knowledge - variant mode not supported
    // Use strict mode for exploded views regardless of global setting
    val effectiveMode = sourceConfig.schema.mode match {
      case SchemaMode.Variant =>
        log.warn("Exploded view '{}' requires schema knowledge; using strict mode instead of variant",
          tableName)
        SchemaMode.Strict
      case SchemaMode.Strict => SchemaMode.Strict
    }

    // Array item schema: wrap in object keyed by arrayFieldName
    // For object arrays, this produces prefixed fields (e.g., addresses_city)
    // For primitive arrays, this produces a single field (e.g., tags)
    val itemSchema = SchemaMapper.toArrowSchemaWithMode(
      OpenAPISchema.ObjectType(Map(arrayFieldName -> arrayFieldInfo.itemSchema), Set.empty),
      sourceConfig.schema.flattenDepth,
      effectiveMode
    )

    // Parent scalar fields (exclude the array field itself)
    val parentSchema = SchemaMapper.toArrowSchemaWithMode(
      OpenAPISchema.ObjectType(
        endpoint.responseSchema.properties.filterNot(_._1 == arrayFieldName),
        endpoint.responseSchema.required
      ),
      sourceConfig.schema.flattenDepth,
      effectiveMode
    )

    // Combine parent fields with exploded item fields
    val combinedFields = new java.util.ArrayList[org.apache.arrow.vector.types.pojo.Field]()
    combinedFields.addAll(parentSchema.getFields)
    combinedFields.addAll(itemSchema.getFields)

    val arrow = new ArrowSchema(combinedFields)
    val spark = ArrowSchemaConverter.toSparkSchema(arrow)
    (arrow, spark)
  }

  override def name(): String = tableName

  override def schema(): StructType = sparkSchema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new ExplodedArrayScanBuilder(this, arrowSchema)
}

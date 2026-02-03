package com.apilytics.spark

import com.apilytics.core.config.{SourceConfig, TableConfig}
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

  // Build schema: parent scalar fields + exploded array element field
  private lazy val (arrowSchema, sparkSchema) = buildExplodedSchema()

  private def buildExplodedSchema(): (ArrowSchema, StructType) = {
    // Get schema for the array item
    val itemSchema = arrayFieldInfo.itemSchema match {
      case obj: OpenAPISchema.ObjectType =>
        // For object arrays, flatten the object fields with array field name prefix
        SchemaMapper.toArrowSchema(
          OpenAPISchema.ObjectType(
            Map(arrayFieldName -> obj),
            Set.empty
          ),
          sourceConfig.schema.flattenDepth
        )
      case _ =>
        // For primitive arrays, the field is just the array field name with the primitive type
        SchemaMapper.toArrowSchema(
          OpenAPISchema.ObjectType(
            Map(arrayFieldName -> arrayFieldInfo.itemSchema),
            Set.empty
          ),
          sourceConfig.schema.flattenDepth
        )
    }

    // Get parent scalar fields (exclude the array field itself)
    val parentSchema = SchemaMapper.toArrowSchema(
      OpenAPISchema.ObjectType(
        endpoint.responseSchema.properties.filterNot(_._1 == arrayFieldName),
        endpoint.responseSchema.required
      ),
      sourceConfig.schema.flattenDepth
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

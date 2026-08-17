package com.apilytics.spark

import com.apilytics.core.config.{SourceConfig, TableConfig}
import com.apilytics.core.source.SourceHandle
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._

class RESTTable(
    val tableName: String,
    val arrowSchema: ArrowSchema,
    val handle: SourceHandle,
    val tableConfig: Option[TableConfig],
    val sourceConfig: SourceConfig,
    val baseUrl: String
) extends Table
    with SupportsRead {

  private lazy val sparkSchema: StructType = ArrowSchemaConverter.toSparkSchema(arrowSchema)

  override def name(): String = tableName

  override def schema(): StructType = sparkSchema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new RESTScanBuilder(this)
}

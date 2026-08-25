package com.apilytics.spark

import com.apilytics.core.config.{CheckpointMode, SourceConfig, TableConfig}
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

  /** MICRO_BATCH_READ is advertised only when the table can actually stream.
    *
    * A streaming source has to answer "how far could I read right now" without reading,
    * and for a pull-based API that is only meaningful when the API takes a "changed
    * since" parameter. Cursor and offset checkpointing cannot say how much is available
    * without fetching it, so claiming the capability for them would let `readStream`
    * plan a query that fails at run time instead of at analysis. */
  private[spark] def supportsMicroBatch: Boolean =
    tableConfig
      .flatMap(_.checkpoint)
      .exists(cc => cc.mode == CheckpointMode.Timestamp && cc.timestampParam.isDefined)

  override def capabilities(): util.Set[TableCapability] = {
    val base = Set(TableCapability.BATCH_READ)
    (if (supportsMicroBatch) base + TableCapability.MICRO_BATCH_READ else base).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new RESTScanBuilder(this)
}

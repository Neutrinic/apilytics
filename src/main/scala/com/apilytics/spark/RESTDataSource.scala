package com.apilytics.spark

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.io.File
import java.util
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/** `spark.read.format("apilytics")` — the same tables as [[RESTCatalog]], without a catalog.
  *
  * The catalog is the main interface: it lists every endpoint as a table and gives each a
  * plain SQL name. Some platforms own the catalog namespace, though. On Databricks, every
  * catalog name on Unity Catalog compute resolves to Unity Catalog and a
  * `spark.sql.catalog.*` plugin is never loaded (#129), while `format(...)` is still
  * honoured. This is the entry point for those platforms (#257).
  *
  * Options: `config`, the HOCON config the catalog would take, and `table`, a table name as
  * the catalog lists it — base, parent-child or exploded. The table comes from the catalog's
  * own `loadTable`, so scans, pushdown, partitioning and streaming are shared, not copied.
  *
  * {{{
  * spark.read.format("apilytics").option("config", "/path/github.conf").option("table", "issues").load()
  * spark.readStream.format("apilytics").option("config", "...").option("table", "events").load()
  * }}}
  */
class RESTDataSource extends TableProvider with DataSourceRegister with StreamSourceProvider {

  override def shortName(): String = "apilytics"

  // Every apilytics table defines schema() as its source of truth; columns() is derived.
  @annotation.nowarn("cat=deprecation")
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    resolve(options.asCaseSensitiveMap()).schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = resolve(properties)

  /** The schema comes from the spec. A user-supplied one could only disagree with what the
    * reader produces, so Spark is told to reject it rather than trust it. */
  override def supportsExternalMetadata(): Boolean = false

  // Streaming for a table that cannot stream.
  //
  // Streamable tables advertise MICRO_BATCH_READ and Spark reads them through the V2 table
  // above. For one that cannot, Spark falls back to this legacy interface; without it the
  // error is "Data source apilytics does not support streamed reading", which is wrong
  // about apilytics and silent about the fix.
  //
  // sourceSchema must still answer for streamable tables. Because this class implements
  // StreamSourceProvider, readStream.load() builds the legacy relation eagerly, calling
  // sourceSchema, before it picks the V2 path. Throwing there unconditionally made every
  // table unstreamable. createSource is reached only when V2 is not used, so it always
  // throws.

  @annotation.nowarn("cat=deprecation")
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]
  ): (String, StructType) = {
    val table = resolve(parameters.asJava)
    if (!table.capabilities().contains(TableCapability.MICRO_BATCH_READ)) throw notStreamable(parameters)
    (shortName(), table.schema())
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]
  ): Source = throw notStreamable(parameters)

  private def notStreamable(parameters: Map[String, String]): IllegalArgumentException =
    new IllegalArgumentException(RESTScan.notStreamable(parameters.getOrElse("table", "<unset>")))

  private def resolve(options: util.Map[String, String]): Table = {
    val opts   = new CaseInsensitiveStringMap(options)
    val config = option(opts, "config", "the path to an apilytics HOCON config file")
    val table  = option(opts, "table", "the name of a table defined by that config")
    val catalog = RESTDataSource.catalogFor(config)
    try catalog.loadTable(Identifier.of(Array("default"), table))
    catch {
      case _: NoSuchTableException =>
        val known = catalog.listTables(Array("default")).map(_.name()).sorted
        throw new IllegalArgumentException(
          s"Option 'table' is '$table', but config '$config' defines no such table. " +
            s"Tables it defines: ${known.mkString(", ")}"
        )
    }
  }

  private def option(opts: CaseInsensitiveStringMap, key: String, what: String): String =
    Option(opts.get(key)).filter(_.trim.nonEmpty).getOrElse(
      throw new IllegalArgumentException(
        s"format(\"apilytics\") requires option '$key': $what. " +
          s"""For example: .option("$key", ...)"""
      )
    )
}

object RESTDataSource {

  /** One initialised catalog per config file, per JVM.
    *
    * A catalog reads its config once per session and serves every table from it; this
    * entry point receives the config on every read. Without a cache, each read would reload
    * the config and re-parse the spec, which for a large spec (GitHub's is ~10 MB) costs
    * seconds per query. Keyed by the file's modification time as well as its path, so an
    * edited config takes effect on the next read.
    */
  private val catalogs = new ConcurrentHashMap[(String, Long), RESTCatalog]()

  private[spark] def catalogFor(configPath: String): RESTCatalog = {
    val key = (configPath, new File(configPath).lastModified())
    val catalog = catalogs.computeIfAbsent(key, _ => {
      val c = new RESTCatalog
      c.initialize("apilytics", new CaseInsensitiveStringMap(Map("config" -> configPath).asJava))
      c
    })
    // Drop catalogs for earlier versions of this file. Done after computeIfAbsent, never
    // inside it: the mapping function must not modify the map, and a removal there can
    // detach the bin the new entry is inserted into, losing it from the cache.
    catalogs.keySet().asScala.filter(k => k._1 == configPath && k != key).foreach(catalogs.remove)
    catalog
  }

  /** Catalogs cached for one config file. Exposed for tests. */
  private[spark] def cachedFor(configPath: String): Int =
    catalogs.keySet().asScala.count(_._1 == configPath)
}

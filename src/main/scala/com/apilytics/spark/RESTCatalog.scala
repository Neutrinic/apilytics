package com.apilytics.spark

import com.apilytics.core.config.{ArrayHandling, Loader, SchemaMode, SourceConfig, TableConfig}
import com.apilytics.core.openapi.Endpoint
import com.apilytics.core.rest.RestSourceCatalog
import com.apilytics.core.schema.SourceSchema
import com.apilytics.core.schema.SchemaMapper
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.slf4j.LoggerFactory

import java.util
import scala.jdk.CollectionConverters._

class RESTCatalog extends CatalogPlugin with TableCatalog with SupportsNamespaces {

  private val log = LoggerFactory.getLogger(getClass)

  private var catalogName: String = _
  private var config: SourceConfig = _

  /** Discovery lives behind the neutral source interface — this catalog no longer knows
    * that tables come from an OpenAPI spec (#191). */
  private var source: RestSourceCatalog = _

  private def effectiveBaseUrl: String = source.baseUrl

  override def name(): String = catalogName

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    val configPath = options.get("config")
    require(configPath != null, s"Catalog '$name' requires 'config' option pointing to a HOCON config file")
    this.config = Loader.load(configPath)
    this.source = new RestSourceCatalog(config)
  }

  // -- TableCatalog --

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    validateNamespace(namespace)
    tableNames.map(t => Identifier.of(namespace, t)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val tableName = ident.name()

    // Check if this is a parent-child table (explicitly configured with parent-table)
    config.tables.get(tableName).filter(_.parentTable.isDefined) match {
      case Some(tc) =>
        loadParentChildTable(ident, tableName, tc)
      case None =>
        // Check if this is an exploded array view (e.g., "customers_tags")
        explodedTableInfo(tableName) match {
          case Some((baseTableName, arrayField)) =>
            loadExplodedTable(ident, baseTableName, arrayField)
          case None =>
            loadBaseTable(ident, tableName)
        }
    }
  }

  private def loadBaseTable(ident: Identifier, tableName: String): Table = {
    // Endpoint matching, config-path overrides, synthetic endpoints for config-only
    // tables and record-schema resolution all happen in the source catalog (#191).
    val spec = source.table(tableName).getOrElse(throw new NoSuchTableException(ident))

    val arrowSchema = SchemaMapper.toArrowSchemaWithMode(
      spec.schema,
      config.schema.flattenDepth,
      config.schema.mode
    )

    new RESTTable(
      tableName = tableName,
      arrowSchema = arrowSchema,
      handle = spec.handle,
      tableConfig = config.tables.get(tableName),
      sourceConfig = config,
      baseUrl = effectiveBaseUrl
    )
  }

  private def loadParentChildTable(ident: Identifier, tableName: String, tableConfig: TableConfig): Table = {
    val parentTableName = tableConfig.parentTable.getOrElse(
      throw new IllegalArgumentException(s"Table '$tableName' missing parent-table config")
    )
    val parentKey = tableConfig.parentKey.getOrElse(
      throw new IllegalArgumentException(s"Table '$tableName' missing parent-key config")
    )

    // Find parent endpoint
    val parentSpec = source.table(parentTableName).getOrElse(
      throw new NoSuchTableException(Identifier.of(Array("default"), parentTableName))
    )

    // Find child endpoint by matching the path template against OpenAPI spec endpoints.
    // The config endpoint has concrete path params (e.g., "/customers/{customer_id}/orders")
    // which should match the spec's parameterized path.
    val childEndpoint = findEndpointByPathTemplate(tableConfig.endpoint).getOrElse(
      throw new IllegalArgumentException(
        s"Child endpoint '${tableConfig.endpoint}' not found in OpenAPI spec for table '$tableName'"
      )
    )

    log.debug("Loading parent-child table '{}': parent='{}', key='{}', child endpoint='{}'",
      tableName, parentTableName, parentKey, childEndpoint.path)

    new ParentChildTable(
      tableName = tableName,
      childEndpointTemplate = tableConfig.endpoint,
      parentTableName = parentTableName,
      parentKey = parentKey,
      parentHandle = parentSpec.handle,
      childResponseSchema = childEndpoint.responseSchema,
      tableConfig = tableConfig,
      sourceConfig = config,
      baseUrl = effectiveBaseUrl
    )
  }

  private def loadExplodedTable(ident: Identifier, baseTableName: String, arrayFieldName: String): Table = {
    val tableConfig = config.tables.get(baseTableName)
    val baseSpec = source.table(baseTableName).getOrElse(throw new NoSuchTableException(ident))

    // Verify the array field exists
    val arrayFields = SchemaMapper.findArrayFields(baseSpec.schema)
    val arrayFieldInfo = arrayFields.find(_.fieldName == arrayFieldName).getOrElse(
      throw new NoSuchTableException(ident)
    )

    new ExplodedArrayTable(
      tableName = ident.name(),
      baseTableName = baseTableName,
      arrayFieldName = arrayFieldName,
      arrayFieldInfo = arrayFieldInfo,
      recordSchema = baseSpec.schema,
      handle = baseSpec.handle,
      tableConfig = tableConfig,
      sourceConfig = config,
      baseUrl = effectiveBaseUrl
    )
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]
  ): Table = throw new UnsupportedOperationException("REST catalog is read-only")

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException("REST catalog is read-only")

  override def dropTable(ident: Identifier): Boolean =
    throw new UnsupportedOperationException("REST catalog is read-only")

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException("REST catalog is read-only")

  override def tableExists(ident: Identifier): Boolean =
    tableNames.contains(ident.name())

  // -- SupportsNamespaces --

  override def listNamespaces(): Array[Array[String]] = Array(Array("default"))

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    if (namespace.isEmpty) listNamespaces()
    else throw new NoSuchNamespaceException(namespace)
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    validateNamespace(namespace)
    java.util.Collections.emptyMap()
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit =
    throw new UnsupportedOperationException("REST catalog is read-only")

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    throw new UnsupportedOperationException("REST catalog is read-only")

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    throw new UnsupportedOperationException("REST catalog is read-only")

  // -- private --

  private def tableNames: Seq[String] = {
    val baseTables = source.tableNames

    // Generate exploded view names if array-handling is explode_view or both
    // Use effective schema (after data-path extraction) to find array fields
    val explodedTables = config.schema.arrayHandling match {
      case ArrayHandling.ExplodeView | ArrayHandling.Both =>
        baseTables.flatMap { tableName =>
          effectiveSchemaForTable(tableName).toSeq.flatMap { schema =>
            SchemaMapper.findArrayFields(schema).map { arrayField =>
              s"${tableName}_${arrayField.fieldName}"
            }
          }
        }
      case ArrayHandling.KeepArray => Nil
    }

    // For ExplodeView mode, only show exploded tables; for Both, show all
    config.schema.arrayHandling match {
      case ArrayHandling.ExplodeView => explodedTables
      case ArrayHandling.Both => baseTables ++ explodedTables
      case ArrayHandling.KeepArray => baseTables
    }
  }

  /** Get the effective schema for a table, accounting for data-path extraction.
    * This is the schema used for exploded view generation.
    */
  private def effectiveSchemaForTable(tableName: String): Option[SourceSchema.ObjectType] =
    source.table(tableName).map(_.schema)

  private def findEndpointByPathTemplate(configPath: String): Option[Endpoint] =
    source.findByPathTemplate(configPath)

  /** Parse an exploded table name like "customers_tags" to ("customers", "tags") if valid.
    *
    * Resolution order: tries splits from right to left, preferring longer base table names.
    * For "a_b_c", tries: ("a_b", "c"), then ("a", "b_c").
    * First match where base table exists AND has matching array field wins.
    */
  private def explodedTableInfo(tableName: String): Option[(String, String)] = {
    config.schema.arrayHandling match {
      case ArrayHandling.KeepArray => None
      case ArrayHandling.ExplodeView | ArrayHandling.Both =>
        val parts = tableName.split("_")
        if (parts.length < 2) None
        else {
          // Try splits from right to left (prefer longer base table names)
          val result = (parts.length - 1 to 1 by -1).view.flatMap { splitAt =>
            val baseTableName = parts.take(splitAt).mkString("_")
            val arrayFieldName = parts.drop(splitAt).mkString("_")
            source.table(baseTableName).flatMap { spec =>
              val arrayFields = SchemaMapper.findArrayFields(spec.schema)
              if (arrayFields.exists(_.fieldName == arrayFieldName)) {
                Some((baseTableName, arrayFieldName))
              } else None
            }
          }.headOption

          result.foreach { case (base, field) =>
            log.debug("Resolved '{}' as exploded view: base='{}', array='{}'", tableName, base, field)
          }
          result
        }
    }
  }

  private def validateNamespace(namespace: Array[String]): Unit = {
    if (namespace.length != 1 || namespace.head != "default")
      throw new NoSuchNamespaceException(namespace)
  }
}

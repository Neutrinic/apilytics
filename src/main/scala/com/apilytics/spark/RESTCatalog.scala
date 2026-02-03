package com.apilytics.spark

import com.apilytics.core.config.{ArrayHandling, Loader, SourceConfig}
import com.apilytics.core.openapi.{Endpoint, ParsedSpec, Parser}
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
  private var spec: ParsedSpec = _

  override def name(): String = catalogName

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    val configPath = options.get("config")
    require(configPath != null, s"Catalog '$name' requires 'config' option pointing to a HOCON config file")
    this.config = Loader.load(configPath)
    this.spec = Parser.parse(config.openapi)
  }

  // -- TableCatalog --

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    validateNamespace(namespace)
    tableNames.map(t => Identifier.of(namespace, t)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val tableName = ident.name()

    // Check if this is an exploded array view (e.g., "customers_tags")
    explodedTableInfo(tableName) match {
      case Some((baseTableName, arrayField)) =>
        loadExplodedTable(ident, baseTableName, arrayField)
      case None =>
        loadBaseTable(ident, tableName)
    }
  }

  private def loadBaseTable(ident: Identifier, tableName: String): Table = {
    val tableConfig = config.tables.get(tableName)
    val endpoint = findEndpointForTable(tableName).getOrElse(throw new NoSuchTableException(ident))
    val arrowSchema = SchemaMapper.toArrowSchema(endpoint.responseSchema, config.schema.flattenDepth)

    new RESTTable(
      tableName = tableName,
      arrowSchema = arrowSchema,
      endpoint = endpoint,
      tableConfig = tableConfig,
      sourceConfig = config,
      baseUrl = spec.baseUrl
    )
  }

  private def loadExplodedTable(ident: Identifier, baseTableName: String, arrayFieldName: String): Table = {
    val tableConfig = config.tables.get(baseTableName)
    val endpoint = findEndpointForTable(baseTableName).getOrElse(throw new NoSuchTableException(ident))

    // Verify the array field exists
    val arrayFields = SchemaMapper.findArrayFields(endpoint.responseSchema)
    val arrayFieldInfo = arrayFields.find(_.fieldName == arrayFieldName).getOrElse(
      throw new NoSuchTableException(ident)
    )

    new ExplodedArrayTable(
      tableName = ident.name(),
      baseTableName = baseTableName,
      arrayFieldName = arrayFieldName,
      arrayFieldInfo = arrayFieldInfo,
      endpoint = endpoint,
      tableConfig = tableConfig,
      sourceConfig = config,
      baseUrl = spec.baseUrl
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
    val configured = config.tables.keys.toSeq
    val discovered = spec.endpoints.flatMap { ep =>
      ep.operationId.orElse(ep.path.split("/").filterNot(s => s.startsWith("{") || s.isEmpty).lastOption)
    }
    val baseTables = (configured ++ discovered).distinct

    // Generate exploded view names if array-handling is explode_view or both
    val explodedTables = config.schema.arrayHandling match {
      case ArrayHandling.ExplodeView | ArrayHandling.Both =>
        baseTables.flatMap { tableName =>
          findEndpointForTable(tableName).toSeq.flatMap { endpoint =>
            SchemaMapper.findArrayFields(endpoint.responseSchema).map { arrayField =>
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

  private def findEndpointForTable(tableName: String): Option[Endpoint] = {
    config.tables.get(tableName).flatMap { tc =>
      spec.endpoints.find(_.path == tc.endpoint)
    }.orElse {
      spec.endpoints.find { ep =>
        ep.operationId.contains(tableName) ||
          ep.path.split("/").filterNot(s => s.startsWith("{") || s.isEmpty).lastOption.exists(_.equalsIgnoreCase(tableName))
      }
    }
  }

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
            findEndpointForTable(baseTableName).flatMap { endpoint =>
              val arrayFields = SchemaMapper.findArrayFields(endpoint.responseSchema)
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

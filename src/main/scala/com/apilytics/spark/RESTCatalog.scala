package com.apilytics.spark

import com.apilytics.core.config.{Loader, SourceConfig}
import com.apilytics.core.openapi.{ParsedSpec, Parser}
import com.apilytics.core.schema.SchemaMapper
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._

class RESTCatalog extends CatalogPlugin with TableCatalog with SupportsNamespaces {

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
    val tableConfig = config.tables.get(tableName)

    // Find matching endpoint: explicit config or discovered from spec
    val endpoint = tableConfig.map { tc =>
      spec.endpoints.find(_.path == tc.endpoint).getOrElse(
        throw new NoSuchTableException(ident)
      )
    }.orElse {
      spec.endpoints.find { ep =>
        ep.operationId.contains(tableName) ||
          ep.path.split("/").last.equalsIgnoreCase(tableName)
      }
    }.getOrElse(throw new NoSuchTableException(ident))

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
      ep.operationId.orElse(Some(ep.path.split("/").last)).filter(_.nonEmpty)
    }
    (configured ++ discovered).distinct
  }

  private def validateNamespace(namespace: Array[String]): Unit = {
    if (namespace.length != 1 || namespace.head != "default")
      throw new NoSuchNamespaceException(namespace)
  }
}

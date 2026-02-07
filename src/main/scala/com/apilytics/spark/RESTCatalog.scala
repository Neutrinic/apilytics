package com.apilytics.spark

import com.apilytics.core.config.{ArrayHandling, Loader, SourceConfig, TableConfig}
import com.apilytics.core.openapi.{Endpoint, OpenAPISchema, ParsedSpec, Parser}
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

  /** Returns the effective base URL, preferring config override over OpenAPI spec. */
  private def effectiveBaseUrl: String = config.baseUrl.getOrElse(spec.baseUrl)

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
    val tableConfig = config.tables.get(tableName)
    val specEndpoint = findEndpointForTable(tableName).getOrElse(throw new NoSuchTableException(ident))

    // If table has explicit config endpoint (potentially with concrete path params),
    // use that path instead of the spec's parameterized path.
    // e.g., config has "/repos/octocat/Hello-World/issues", spec has "/repos/{owner}/{repo}/issues"
    val endpoint = tableConfig.map(tc => specEndpoint.copy(path = tc.endpoint)).getOrElse(specEndpoint)

    // If data-path is configured, extract the schema at that path
    // e.g., data-path = "/results" means get the item schema from the results array
    // Also automatically unwrap synthetic array wrappers (single "data" array property)
    val effectiveSchema = tableConfig.flatMap(_.dataPath) match {
      case Some(dataPath) =>
        extractSchemaAtPath(endpoint.responseSchema, dataPath).getOrElse {
          log.warn("Could not extract schema at data-path '{}' for table '{}', using full response schema",
            dataPath, tableName)
          endpoint.responseSchema
        }
      case None =>
        // Check if this is a synthetic array wrapper from Parser (single "data" array prop)
        // If so, automatically extract the item schema
        endpoint.responseSchema.properties.get("data") match {
          case Some(OpenAPISchema.ArrayType(obj: OpenAPISchema.ObjectType))
              if endpoint.responseSchema.properties.size == 1 =>
            obj
          case _ =>
            endpoint.responseSchema
        }
    }

    val arrowSchema = SchemaMapper.toArrowSchema(effectiveSchema, config.schema.flattenDepth)

    new RESTTable(
      tableName = tableName,
      arrowSchema = arrowSchema,
      endpoint = endpoint,
      tableConfig = tableConfig,
      sourceConfig = config,
      baseUrl = effectiveBaseUrl
    )
  }

  /** Extract the schema at a JSON pointer path.
    * For array paths, returns the item schema.
    * E.g., "/results" on {results: array[Item]} returns Item schema.
    */
  private def extractSchemaAtPath(schema: OpenAPISchema.ObjectType, path: String): Option[OpenAPISchema.ObjectType] = {
    val segments = path.stripPrefix("/").split("/").filter(_.nonEmpty).toList

    def navigate(current: OpenAPISchema, remaining: List[String]): Option[OpenAPISchema.ObjectType] = {
      remaining match {
        case Nil =>
          current match {
            case obj: OpenAPISchema.ObjectType => Some(obj)
            case arr: OpenAPISchema.ArrayType =>
              arr.items match {
                case obj: OpenAPISchema.ObjectType => Some(obj)
                case _ => None
              }
            case _ => None
          }
        case segment :: rest =>
          current match {
            case obj: OpenAPISchema.ObjectType =>
              obj.properties.get(segment).flatMap(navigate(_, rest))
            case arr: OpenAPISchema.ArrayType =>
              navigate(arr.items, segment :: rest)
            case _ => None
          }
      }
    }

    navigate(schema, segments)
  }

  private def loadParentChildTable(ident: Identifier, tableName: String, tableConfig: TableConfig): Table = {
    val parentTableName = tableConfig.parentTable.getOrElse(
      throw new IllegalArgumentException(s"Table '$tableName' missing parent-table config")
    )
    val parentKey = tableConfig.parentKey.getOrElse(
      throw new IllegalArgumentException(s"Table '$tableName' missing parent-key config")
    )

    // Find parent endpoint
    val parentEndpoint = findEndpointForTable(parentTableName).getOrElse(
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
      parentEndpoint = parentEndpoint,
      childResponseSchema = childEndpoint.responseSchema,
      tableConfig = tableConfig,
      sourceConfig = config,
      baseUrl = effectiveBaseUrl
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
      // First try exact match, then try template matching for parameterized paths
      // e.g., config "/repos/octocat/Hello-World/issues" matches spec "/repos/{owner}/{repo}/issues"
      spec.endpoints.find(_.path == tc.endpoint)
        .orElse(findEndpointByPathTemplate(tc.endpoint))
    }.orElse {
      spec.endpoints.find { ep =>
        ep.operationId.contains(tableName) ||
          ep.path.split("/").filterNot(s => s.startsWith("{") || s.isEmpty).lastOption.exists(_.equalsIgnoreCase(tableName))
      }
    }
  }

  /** Find an endpoint by matching a path template against OpenAPI spec endpoints.
    *
    * Matches config paths (which may have concrete values or `{param}` placeholders) against
    * OpenAPI spec paths (which use `{param}` syntax for path parameters).
    *
    * A spec `{param}` segment matches:
    * - Any concrete value (e.g., "octocat" matches "{owner}")
    * - Any other `{...}` placeholder (e.g., "{customer_id}" matches "{id}")
    *
    * Example: "/repos/octocat/Hello-World/issues" matches spec "/repos/{owner}/{repo}/issues"
    * Example: "/customers/{customer_id}/orders" matches spec "/customers/{id}/orders"
    */
  private def findEndpointByPathTemplate(configPath: String): Option[Endpoint] = {
    val configSegments = configPath.split("/").toList

    spec.endpoints.find { ep =>
      val specSegments = ep.path.split("/").toList
      if (configSegments.length != specSegments.length) false
      else {
        configSegments.zip(specSegments).forall { case (configSeg, specSeg) =>
          // Spec param matches any config segment (concrete or placeholder)
          // Otherwise require exact match
          specSeg.startsWith("{") || configSeg == specSeg
        }
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

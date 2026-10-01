# Installation

APIlytics is published to Maven Central as `io.github.neutrinic:apilytics_2.13`.

```bash
--8<-- "packages.sh"
```

For an application built with sbt:

```scala
--8<-- "sbt.scala"
```

Spark itself is a provided dependency and is not pulled in.

## Register a catalog

Each API you query is a Spark catalog, configured by a HOCON file. See
[Configuration](../configuration/index.md) for what goes in it.

```scala
spark.conf.set("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
spark.conf.set("spark.sql.catalog.api.config", "/path/to/config.conf")
spark.sql("SELECT * FROM api.default.issues LIMIT 5").show()
```

Or on the command line, which works for `spark-sql`, `pyspark` and `spark-submit` too:

```bash
--8<-- "catalog-conf.sh"
```

Tables are always addressed as `<catalog>.default.<table>`. See
[Catalogs](../using/catalogs.md).

## Without a catalog: `format("apilytics")`

Some platforms own the catalog namespace. On Databricks, every catalog name on Unity
Catalog compute resolves to Unity Catalog, so the catalog above is never loaded there. The
same tables are available as a data source instead:

```python
issues = (spark.read.format("apilytics")
          .option("config", "/path/to/config.conf")
          .option("table", "issues")
          .load())
```

```sql
CREATE TEMPORARY VIEW issues USING apilytics
OPTIONS (config '/path/to/config.conf', table 'issues');
```

It's the same table the catalog serves, with the same pushdown, partitioning and streaming
(`spark.readStream.format("apilytics")`). What it gives up is discovery: there's no
`SHOW TABLES`, so you name the table you want. Use the catalog wherever the platform
allows it.

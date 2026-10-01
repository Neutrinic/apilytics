# Catalogs

Each API is a Spark catalog. Its tables always live in the `default` namespace, so they
are addressed as `<catalog>.default.<table>`. `<catalog>.<table>` does not work.

## Exploring a catalog

Catalogs are loaded lazily, so a catalog doesn't appear in `SHOW CATALOGS` until something
has used it:

```sql
-- Any query against the catalog registers it
SHOW NAMESPACES IN api;

-- Now it appears
SHOW CATALOGS;

SHOW TABLES IN api.default;
DESCRIBE api.default.issues;
DESCRIBE EXTENDED api.default.issues;
```

## Several APIs at once

Register each API as its own catalog:

```bash
spark-shell \
  --conf spark.sql.catalog.github=com.apilytics.spark.RESTCatalog \
  --conf spark.sql.catalog.github.config=/path/to/github-config.conf \
  --conf spark.sql.catalog.slack=com.apilytics.spark.RESTCatalog \
  --conf spark.sql.catalog.slack.config=/path/to/slack-config.conf
```

```sql
SELECT number, title FROM github.default.issues LIMIT 5;
SELECT name FROM slack.default.channels LIMIT 5;
```

## Every query calls the API

Each query pays for the API calls, pagination, rate limits and network latency. For
repeated analysis, cache the table or write it out once:

```scala
// Cache once, then query at Spark speed
val issues = spark.table("api.default.issues").cache()

issues.groupBy("state").count().show()
issues.filter("state = 'open'").orderBy(desc("created_at")).show()

// Or persist it as a table
issues.write.saveAsTable("warehouse.github_issues")
```

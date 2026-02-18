# APIlytics

[![Build](https://github.com/Neutrinic/apilytics/actions/workflows/ci.yml/badge.svg)](https://github.com/Neutrinic/apilytics/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.neutrinic/apilytics_2.13.svg)](https://central.sonatype.com/artifact/io.github.neutrinic/apilytics_2.13)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Scala](https://img.shields.io/badge/Scala-2.13-red.svg)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Spark-4.0-orange.svg)](https://spark.apache.org/)

Turn any REST API with an OpenAPI spec into queryable Apache Spark tables.

## Installation

### Maven Central

```scala
// build.sbt
libraryDependencies += "io.github.neutrinic" %% "apilytics" % "0.6.0"
```

```bash
# spark-submit
spark-submit --packages io.github.neutrinic:apilytics_2.13:0.6.0 your-app.jar

# spark-shell
spark-shell --packages io.github.neutrinic:apilytics_2.13:0.6.0
```

Then configure the catalog:

```scala
spark.conf.set("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog")
spark.conf.set("spark.sql.catalog.api.config", "/path/to/config.conf")
spark.sql("SELECT * FROM api.default.issues LIMIT 5").show()
```

### Docker

Try Apilytics instantly with Docker - no Java, Scala, or build tools required:

```bash
docker run -it --rm ghcr.io/neutrinic/apilytics:latest "SELECT name FROM api.default.pokemon LIMIT 20"
```
```bash
# Interactive spark-sql shell
docker run -it --rm ghcr.io/neutrinic/apilytics:latest
# Interactive spark-sql shell with bundled Github
docker run -it --rm ghcr.io/neutrinic/apilytics:latest --config /opt/apilytics/examples/github/github-config.conf
# With your own config file
docker run -it --rm -v /path/to/my.conf:/config.conf ghcr.io/neutrinic/apilytics:latest --config /config.conf

# Additional help
docker run --rm ghcr.io/neutrinic/apilytics --help
```

Bundled configs:
- `examples/pokeapi/pokeapi-config.conf` (default) - PokeAPI, no auth
- `examples/github/github-config.conf` - GitHub public repos, no auth (60 req/hour limit)

### PySpark

The Docker image includes Python for PySpark:

```bash
# Interactive PySpark shell
docker run -it --rm ghcr.io/neutrinic/apilytics:latest pyspark

# Run a Python script
docker run -it --rm ghcr.io/neutrinic/apilytics:latest spark-submit /opt/apilytics/examples/pyspark/basic.py
```

### Jupyter Notebooks

Start a Jupyter notebook server with pre-configured catalogs:

```bash
docker run -p 8888:8888 --rm ghcr.io/neutrinic/apilytics:latest jupyter
```

Open http://localhost:8888 (token shown in logs). Example notebooks are included in `/opt/apilytics/examples/notebooks/`.

### Thrift/JDBC

Start a Thrift server for JDBC access (DBeaver, Tableau, PowerBI):

```bash
docker run -p 10000:10000 --rm ghcr.io/neutrinic/apilytics:latest thrift
```

Connect with JDBC URL `jdbc:hive2://localhost:10000`. Both `pokeapi` and `github` catalogs are pre-configured.

## Development Setup

For local development or contributing:

```bash
# Build the JAR
sbt assembly

# Start Spark cluster with GitHub API example
./scripts/spark-shell.sh github
```

```scala
// Query GitHub issues
spark.sql("SELECT number, title, state FROM api.default.issues LIMIT 10").show()
```

Every query hits the API — pagination, rate limits, network latency. For repeated analysis, cache locally:

```scala
// Cache once, query at Spark speed
val issues = spark.table("api.default.issues").cache()

issues.groupBy("state").count().show()
issues.filter("state = 'open'").orderBy(desc("created_at")).show()

// Or persist to Delta for permanent storage
issues.write.format("delta").saveAsTable("warehouse.github_issues")
```

### Clean Error Messages

Use `sqlClean()` for user-friendly error messages instead of Spark's verbose stack traces:

```scala
import com.apilytics.spark.implicits._

// SQL with clean errors
spark.sqlClean("SELECT * FROM api.default.issues").show()

// Or wrap any operation in a block
withCleanErrors {
  spark.sql("SELECT * FROM api.default.issues").show()
}
```

### Exploring Catalogs

Catalogs are lazily loaded - they won't appear in `SHOW CATALOGS` until first accessed. To discover available catalogs and tables:

```sql
-- Touch the catalog to register it (any query works)
SHOW NAMESPACES IN api;

-- Now it appears in SHOW CATALOGS
SHOW CATALOGS;

-- List tables (always use <catalog>.default.<table> format)
SHOW TABLES IN api.default;

-- Describe schema
DESCRIBE api.default.issues;
DESCRIBE EXTENDED api.default.issues;
```

**Important**: Always use the full path `<catalog>.default.<table>` format. Using just `<catalog>.<table>` won't work - the `default` namespace is required.

### Multiple Catalogs

You can load multiple APIs as separate catalogs:

```bash
# Start with both GitHub and PokeAPI
./scripts/spark-shell.sh multi
```

```sql
-- Touch each catalog to register it
SHOW NAMESPACES IN github;
SHOW NAMESPACES IN pokemon;

-- Now both appear
SHOW CATALOGS;

-- Query tables (always use <catalog>.default.<table>)
SELECT number, title FROM github.default.issues LIMIT 5;
SELECT name, url FROM pokemon.default.pokemon LIMIT 5;
```

To configure multiple catalogs manually:

```bash
spark-shell \
  --conf "spark.sql.catalog.github=com.apilytics.spark.RESTCatalog" \
  --conf "spark.sql.catalog.github.config=/path/to/github-config.conf" \
  --conf "spark.sql.catalog.slack=com.apilytics.spark.RESTCatalog" \
  --conf "spark.sql.catalog.slack.config=/path/to/slack-config.conf"
```

### JDBC Access (DBeaver, Tableau, PowerBI)

Connect BI tools via standard JDBC using the Thrift Server:

```bash
cd docker/spark
docker compose -f compose.spark.yaml up -d
```

**Connection Details:**
- **Driver**: Apache Hive JDBC
- **URL**: `jdbc:hive2://localhost:10000`
- **Username**: any (e.g., "spark")
- **Password**: empty

**DBeaver Setup:**
1. New Connection > Apache Hive
2. Host: localhost, Port: 10000
3. Test Connection

**Configure Multiple Catalogs** in `compose.spark.yaml`:
```yaml
environment:
  - CATALOGS=github,pokemon
  - CATALOG_GITHUB_CONFIG=/opt/spark/examples/github/github-config.conf
  - CATALOG_POKEMON_CONFIG=/opt/spark/examples/pokeapi/pokeapi-config.conf
```

Then query with catalog prefix:
```sql
SELECT * FROM github.default.issues LIMIT 10;
SELECT * FROM pokemon.default.pokemon LIMIT 10;
```

## Overview

APIlytics is a Spark DataSource V2 catalog plugin that reads OpenAPI specs (Swagger 2.0, OpenAPI 3.0/3.1) and exposes API endpoints as Spark tables. Filters and limits are pushed down to query parameters, pagination is handled automatically, and responses are converted to Arrow columnar format for efficient processing.

## Features

- **OpenAPI parsing** - Swagger 2.0, OpenAPI 3.0/3.1 via swagger-parser; GET endpoints with array responses become tables
- **Pagination** - cursor, offset, and link header strategies with configurable page sizes
- **Authentication** - bearer token, basic auth, custom headers, OAuth2 client credentials
- **Filter pushdown** - Spark SQL filters map to API query parameters
- **Limit pushdown** - stops pagination early when a LIMIT clause is present
- **Aggregation pushdown** - SUM, AVG, MIN, MAX, COUNT, and custom functions push to API endpoints
- **Schema modes** - strict (default, typed columns) or variant (native VARIANT for schema-free queries)
- **Schema flattening** - nested objects flatten to a configurable depth, deeper nesting falls back to STRING
- **Arrow internals** - zero-copy path to Spark ColumnarBatch
- **Parent-child joins** - chain API calls (e.g., fetch issues then comments for each)
- **Batch joins** - reduce API calls from O(n) to O(n/batch_size) for bulk lookups
- **Parallel partitioning** - date-range or enum partitioning for concurrent reads
- **Rate limiting** - configurable requests per second with automatic distribution across partitions
- **Retry with backoff** - exponential backoff for transient failures (429, 5xx)

## Logging & Debugging

APIlytics logs filter pushdown decisions at INFO level. To enable in spark-shell:

```scala
import org.apache.log4j.{Logger, Level}
Logger.getLogger("com.apilytics").setLevel(Level.INFO)
Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
```

When you run a filtered query, you'll see which filters are pushed to the API:

```
INFO FilterPushdown: Filters pushed to API: state = 'open'
```

Filters without a matching `filters` config are applied locally by Spark:

```
INFO FilterPushdown: Filters applied locally by Spark: author = 'octocat'
```

This helps debug slow queries — pushed filters reduce API calls, while local filters scan all pages.

## Configuration

See [`examples/github/github-config.conf`](examples/github/github-config.conf) for a complete configuration reference with all options documented.

```hocon
# Minimal example
openapi = "https://api.example.com/openapi.json"

auth {
  type = "bearer"
  token = ${API_TOKEN}
}

pagination {
  style = "link_header"       # link_header | cursor | offset | none
  page-size-param = "per_page"
  max-page-size = 100
}

http {
  timeout = "30s"
  max-retries = 3
  max-backoff = "30s"
  response-format = "json"  # json | ndjson | sse
}

schema {
  flatten-depth = 2           # 0 = no flattening, nested objects become JSON
  array-handling = "both"     # keep_array | explode_view | both
  mode = "strict"             # strict | variant
}

tables {
  issues {
    endpoint = "/repos/owner/repo/issues"
    filters = [
      { param = "state", column = "state", operators = ["eq"] }
    ]
  }
}
```

The script registers the catalog as `api`, so queries use `api.default.<table>`.

### Schema Modes

Control how OpenAPI schemas are used with the `mode` setting:

| Mode | Description |
|------|-------------|
| `strict` | (Default) Use OpenAPI schema with flattening. Typed columns, nested objects beyond depth become STRING. |
| `variant` | Return entire response as native Spark VARIANT column. Faster than JSON strings ([benchmark](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)). |

**Strict mode** (default) uses the OpenAPI schema to produce typed columns. Nested objects are flattened to `flatten-depth`, deeper nesting becomes JSON strings which you can parse with `parse_json()` if needed.

**Variant mode** returns the entire response as a native Spark 4.0 VARIANT column with binary encoding:

```hocon
schema {
  mode = "variant"
}
```

```sql
-- Returns single "value" column of type VARIANT
SELECT value FROM api.default.users LIMIT 5;

-- Use variant_get() for typed field access (no parse_json needed!)
SELECT
  variant_get(value, '$.name', 'STRING') as name,
  variant_get(value, '$.email', 'STRING') as email
FROM api.default.users;

-- Nested paths work too
SELECT
  variant_get(value, '$.address.city', 'STRING') as city,
  variant_get(value, '$.stats[0].value', 'INT') as first_stat
FROM api.default.users;
```

> **Note**: The colon syntax (`value:name::string`) shown in Databricks documentation is Databricks-specific. Open source Spark 4.0 uses `variant_get()` function.

### Streaming Response Formats

By default, APIlytics expects full-body JSON responses. For APIs that stream data, configure `response-format`:

| Format | Content-Type | Description |
|--------|--------------|-------------|
| `json` | `application/json` | (Default) Full-body JSON response |
| `ndjson` | `application/x-ndjson` | Newline-delimited JSON (JSON Lines). One record per line. |
| `sse` | `text/event-stream` | Server-Sent Events. Parses `data:` fields as JSON. |

```hocon
http {
  response-format = "ndjson"  # For BigQuery exports, Elasticsearch scroll, etc.
}
```

**When to use streaming formats:**
- **NDJSON**: BigQuery export, Elasticsearch scroll API, CouchDB changes feed
- **SSE**: Real-time feeds, change data capture streams

Streaming formats bypass pagination since they represent continuous data streams. Apply `LIMIT` in SQL to cap the number of records read.

## Building

Requires Java 17+ and sbt.

```bash
sbt compile
sbt assembly  # fat JAR for Spark
```

## Docker

A Spark standalone cluster is provided for local development:

```bash
# Start cluster
cd docker/spark
docker compose -f compose.spark.yaml up -d

# Or use the helper script
./scripts/spark-shell.sh github
```

This starts a Spark master, two workers, and a history server. The assembled JAR is mounted automatically.

## Architecture

```
Config (HOCON) → OpenAPI Parser → Schema Mapper → Arrow Schema
                                                        ↓
Spark Catalog ← Tables ← ScanBuilder (pushdown) → HTTP Client
                                                        ↓
                                              Paginator (fs2 Stream)
                                                        ↓
                                              Arrow Converter → ColumnarBatch
```

## Stack

- Scala 2.13 / Spark 4.0
- http4s-ember-client (HTTP)
- circe (JSON) + circe-pointer (RFC 6901)
- swagger-parser (OpenAPI)
- Apache Arrow (columnar format)
- pureconfig (HOCON config)
- fs2 (streaming pagination)

## License

Apache License 2.0

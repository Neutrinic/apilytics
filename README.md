# APIlytics

[![Build](https://github.com/Neutrinic/apilytics/actions/workflows/ci.yml/badge.svg)](https://github.com/Neutrinic/apilytics/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Scala](https://img.shields.io/badge/Scala-2.13-red.svg)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Spark-4.0-orange.svg)](https://spark.apache.org/)

Turn any REST API with an OpenAPI spec into queryable Apache Spark tables.

## Quick Start

```bash
# Build the JAR
sbt assembly

# Start Spark cluster with GitHub API example
./scripts/spark-shell.sh github
```

```scala
// List available tables
spark.sql("SHOW TABLES IN api.default").show()

// Query GitHub issues
spark.sql("SELECT number, title, state FROM api.default.issues LIMIT 10").show()

// Filter and aggregate
spark.sql("""
  SELECT state, COUNT(*) as count
  FROM api.default.issues
  GROUP BY state
""").show()
```

## Overview

APIlytics is a Spark DataSource V2 catalog plugin that reads OpenAPI 3.x specifications and exposes API endpoints as Spark tables. Filters and limits are pushed down to query parameters, pagination is handled automatically, and responses are converted to Arrow columnar format for efficient processing.

## Features

- **OpenAPI 3.x** parsing via swagger-parser - GET endpoints with array responses become tables
- **Pagination** - cursor, offset, and link header strategies with configurable page sizes
- **Authentication** - bearer token, basic auth, custom headers, OAuth2 client credentials
- **Filter pushdown** - Spark SQL filters map to API query parameters
- **Limit pushdown** - stops pagination early when a LIMIT clause is present
- **Schema flattening** - nested objects flatten to a configurable depth, deeper nesting falls back to VARIANT
- **Arrow internals** - zero-copy path to Spark ColumnarBatch
- **Parent-child joins** - chain API calls (e.g., fetch issues then comments for each)

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
}

schema {
  flatten-depth = 2           # 0 = no flattening, nested objects become JSON
  array-handling = "both"     # keep_array | explode_view | both
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

# APIlytics

Turn any REST API with an OpenAPI spec into queryable Apache Spark tables.

```sql
CREATE CATALOG stripe USING apilytics OPTIONS (config '/path/to/stripe.conf')

SELECT * FROM stripe.customers WHERE email = 'user@example.com' LIMIT 10
```

## Overview

APIlytics is a Spark DataSource V2 catalog plugin that reads OpenAPI 3.x specifications and exposes API endpoints as Spark tables. Filters and limits are pushed down to query parameters, pagination is handled automatically, and responses are converted to Arrow columnar format for efficient processing.

## Features

- **OpenAPI 3.x** parsing via swagger-parser — GET endpoints with array responses become tables
- **Pagination** — cursor, offset, and link header strategies with configurable page sizes
- **Authentication** — bearer token, basic auth, custom headers, OAuth2 client credentials
- **Filter pushdown** — Spark SQL filters map to API query parameters
- **Limit pushdown** — stops pagination early when a LIMIT clause is present
- **Schema flattening** — nested objects flatten to a configurable depth, deeper nesting falls back to VARIANT
- **Arrow internals** — zero-copy path to Spark ColumnarBatch

## Configuration

APIlytics uses HOCON configuration with native environment variable substitution:

```hocon
openapi = "https://api.stripe.com/v1/openapi.json"

auth {
  type = bearer
  token = ${STRIPE_API_KEY}
}

pagination {
  style = cursor
  cursor-path = "/meta/next_cursor"  # RFC 6901 JSON Pointer
  cursor-param = "starting_after"
  page-size-param = "limit"
  max-page-size = 100
}

schema {
  flatten-depth = 2
  array-handling = keep_array  # keep_array | explode_view | both
}

http {
  max-retries = 5
  max-backoff = 30s
  timeout = 30s
}

tables {
  customers {
    endpoint = "/v1/customers"
    data-path = "/data"
    filters = [
      { param = "email", column = "email", operators = ["eq"] }
      { param = "created[gte]", column = "created_at", operators = ["gte"] }
    ]
  }
}
```

## Building

Requires Java 17+ and sbt.

```bash
sbt compile
sbt assembly  # fat JAR for Spark
```

## Docker

A Spark standalone cluster is provided for local development:

```bash
cd docker/spark
docker compose -f compose.spark.yaml up -d
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

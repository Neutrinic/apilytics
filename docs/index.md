# APIlytics

Turn any REST API with an OpenAPI spec into queryable Apache Spark tables.

```sql
SELECT name FROM api.default.pokemon LIMIT 5;
```

APIlytics is a Spark DataSource V2 catalog. It reads an OpenAPI spec (Swagger 2.0, OpenAPI
3.0 or 3.1) and exposes the API's endpoints as read-only tables:

- filters, limits and aggregates are pushed down to query parameters
- pagination is handled for you
- responses are converted to Arrow columnar batches

It runs on **Spark 4.0, 4.1 and 4.2**, with Scala 2.13 and Java 17 or 21. One jar covers the
line, and every version is built and tested in CI. The jar is self-contained: its
dependencies are bundled and relocated, so they cannot clash with the libraries a Spark
platform ships.

| | 4.0 | 4.1 | 4.2 |
|---|:--:|:--:|:--:|
| SQL, filter / limit / aggregate pushdown | ✅ | ✅ | ✅ |
| Variant mode | ✅ | ✅ | ✅ |
| Parent-child joins, checkpointing | ✅ | ✅ | ✅ |
| Streaming reads | ✅ | ✅ | ✅ |
| Declarative Pipelines | — | ✅ | ✅ |

Declarative Pipelines needs Spark's own support for them, which arrived in 4.1.

--8<-- "version.md"

## Where to start

- **Try it in one command** with Docker: [Quick start](getting-started/quick-start.md).
- **Add it to your Spark**: [Installation](getting-started/installation.md).
- **Run it on your platform**: [Deploying](deploying/index.md), for standalone, YARN, Kubernetes,
  Databricks, EMR and Dataproc.
- **Describe your own API**: [Configuration](configuration/index.md), with every key in the
  [reference](reference/configuration.md).

## Features

- **OpenAPI parsing**: Swagger 2.0 and OpenAPI 3.0/3.1. GET endpoints that return arrays become tables.
- **Pagination**: cursor, offset and link-header styles, with configurable page sizes.
- **Authentication**: bearer token, basic auth and custom headers. See [Credentials](configuration/credentials.md).
- **Pushdown**:
    - **filters** map to API query parameters
    - **limits** stop pagination early
    - **COUNT, SUM and AVG** push to API endpoints. MIN, MAX and custom functions don't, because their result type can't be decided at plan time, so Spark computes them over a full scan.
- **Schema modes**: strict (typed columns, the default) or variant (a native VARIANT column). Nested objects flatten to a configurable depth. See [Schema](configuration/schema.md).
- **Parent-child joins**: chain API calls, such as fetching issues and then each issue's comments. See [Parent-child joins](using/joins.md).
- **Parallel partitioning**: offset, date-range or enum partitioning for concurrent reads. See [Partitioning](using/partitioning.md).
- **Rate limiting**: a ceiling on requests per second, divided across partitions. See [Rate limiting](using/rate-limiting.md).
- **Retries with backoff**: exponential backoff for transient failures (429 and 5xx).
- **Streaming and incremental reads**: micro-batch streaming and Declarative Pipelines. See [Streaming](using/streaming.md) and [Checkpoints](configuration/checkpoints.md).

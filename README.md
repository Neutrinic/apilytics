# APIlytics

[![Build](https://github.com/Neutrinic/apilytics/actions/workflows/ci.yml/badge.svg)](https://github.com/Neutrinic/apilytics/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.neutrinic/apilytics_2.13.svg)](https://central.sonatype.com/artifact/io.github.neutrinic/apilytics_2.13)
[![Docs](https://img.shields.io/badge/docs-neutrinic.github.io-blue.svg)](https://neutrinic.github.io/apilytics/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Spark](https://img.shields.io/badge/Spark-4.0--4.2-orange.svg)](https://spark.apache.org/)

Turn any REST API with an OpenAPI spec into queryable Apache Spark tables.

```sql
SELECT number, title, state FROM api.default.issues WHERE state = 'open' LIMIT 10;
```

Filters, limits and aggregates are pushed down to the API, pagination is handled for you,
and responses arrive as Arrow columnar batches. Runs on **Spark 4.0, 4.1 and 4.2**, with
Scala 2.13 and Java 17 or 21. It's one self-contained jar: its dependencies are bundled
and relocated, so they can't clash with the libraries a Spark platform ships.

| | 4.0 | 4.1 | 4.2 |
|---|:--:|:--:|:--:|
| SQL, filter / limit / aggregate pushdown | ✅ | ✅ | ✅ |
| Variant mode | ✅ | ✅ | ✅ |
| Parent-child joins, checkpointing | ✅ | ✅ | ✅ |
| Streaming reads | ✅ | ✅ | ✅ |
| Declarative Pipelines | — | ✅ | ✅ |

## Quick start

Try it with Docker, without Java or a build tool:

```bash
docker run -it --rm ghcr.io/neutrinic/apilytics:latest "SELECT name FROM api.default.pokemon LIMIT 20"
```

Or add it to your own Spark:

```bash
spark-shell --packages io.github.neutrinic:apilytics_2.13:1.0.1 \
  --conf spark.sql.catalog.api=com.apilytics.spark.RESTCatalog \
  --conf spark.sql.catalog.api.config=/path/to/config.conf
```

The config file names the OpenAPI spec, the authentication and the endpoints to expose.
[`examples/`](examples/) has complete ones for GitHub, PokeAPI, Slack and streaming APIs.

## Documentation

**[neutrinic.github.io/apilytics](https://neutrinic.github.io/apilytics/)** covers
installation, configuration, streaming, Declarative Pipelines, JDBC and BI tools,
partitioning, rate limits, performance and troubleshooting.

Benchmarks, and the synthetic API they read, are in
[apilytics-bench](https://github.com/Neutrinic/apilytics-bench).

To build or contribute, see [Development](https://neutrinic.github.io/apilytics/latest/development/)
and [CONTRIBUTING.md](CONTRIBUTING.md). [GOVERNANCE.md](GOVERNANCE.md) says who maintains the
project and how decisions are made. To report a vulnerability, see [SECURITY.md](SECURITY.md).

## License

Apache License 2.0

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=c8c25824-3523-4562-9b07-1c17e1ac0e48" alt="" width="1" height="1" />

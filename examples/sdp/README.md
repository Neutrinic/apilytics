# Spark Declarative Pipelines

APIlytics works with [Spark Declarative Pipelines](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
(SDP) with **no connector-specific code**.

SDP has no custom source plugin API, which is the thing worth knowing up front. Pipelines
read through ordinary catalogs — `spark.read`, `spark.table()`, SQL — so a DataSource V2
catalog like APIlytics is registered the same way it is for any other Spark job, and its
tables resolve like any other table.

## Run it

```bash
docker run --rm ghcr.io/neutrinic/apilytics:latest \
  spark-pipelines run --spec /opt/apilytics/examples/sdp/spark-pipeline.yml
```

You should see the flow reach `COMPLETED`:

```
Flow spark_catalog.default.first_pokemon is RUNNING.
Flow spark_catalog.default.first_pokemon has COMPLETED.
Run is COMPLETED.
```

Validate the graph without executing it:

```bash
docker run --rm ghcr.io/neutrinic/apilytics:latest \
  spark-pipelines dry-run --spec /opt/apilytics/examples/sdp/spark-pipeline.yml
```

## How the catalog is registered

The SDP CLI has no `--conf` flag, so Spark settings travel in the spec's `configuration`
block rather than on the command line:

```yaml
configuration:
  spark.sql.catalog.api: com.apilytics.spark.RESTCatalog
  spark.sql.catalog.api.config: /path/to/your-config.conf
```

Everything else is ordinary SDP. `transformations/first_pokemon.sql` is a plain
`CREATE MATERIALIZED VIEW` over `api.default.pokemon`.

## Why materialize an API at all

An APIlytics table is a live API call. Every query re-fetches, subject to pagination, rate
limits and network latency. A materialized view turns that into Parquet once, so downstream
queries read at Spark speed and the API sees a single controlled read.

That makes SDP a better fit for API data than for a warehouse table, where the source was
already fast.

## Requirements

SDP runs on Spark Connect, so it needs the Connect Python client — `pyarrow`, `grpcio`,
`grpcio-status`, `googleapis-common-protos` and `zstandard`. The APIlytics image ships
these. In your own environment, install them alongside PySpark or `spark-pipelines` will
fail at import.

Note that classic PySpark does **not** need them, so a working `pyspark` shell is not
evidence that SDP will run.

## Streaming tables

`CREATE STREAMING TABLE ... FROM STREAM` is not supported yet: it needs a micro-batch
source, and APIlytics tables currently declare batch reads only. Tracked in
[#36](https://github.com/Neutrinic/apilytics/issues/36). Materialized views, as here, work
today.

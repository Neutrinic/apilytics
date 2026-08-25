# Spark Declarative Pipelines

Materialize a REST API into a view. Requires Spark 4.1+.

```bash
docker run --rm ghcr.io/neutrinic/apilytics:latest \
  spark-pipelines run --spec /opt/apilytics/examples/sdp/spark-pipeline.yml
```

```
Flow spark_catalog.default.first_pokemon has COMPLETED.
Run is COMPLETED.
```

Validate without running:

```bash
docker run --rm ghcr.io/neutrinic/apilytics:latest \
  spark-pipelines dry-run --spec /opt/apilytics/examples/sdp/spark-pipeline.yml
```

## Configuring the catalog

The SDP CLI takes no `--conf` flags, so Spark settings go in the spec:

```yaml
configuration:
  spark.sql.catalog.api: com.apilytics.spark.RESTCatalog
  spark.sql.catalog.api.config: /path/to/your-config.conf
```

`transformations/first_pokemon.sql` is then an ordinary `CREATE MATERIALIZED VIEW` over
`api.default.pokemon`.

## Requirements

SDP runs on Spark Connect and needs its Python client: `pyarrow`, `grpcio`,
`grpcio-status`, `googleapis-common-protos`, `zstandard`. This image ships them; a plain
PySpark install does not.

## Limitations

`CREATE STREAMING TABLE ... FROM STREAM` needs a micro-batch source, which APIlytics tables
do not yet declare ([#36](https://github.com/Neutrinic/apilytics/issues/36)). Materialized
views work.

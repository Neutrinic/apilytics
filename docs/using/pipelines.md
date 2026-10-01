# Declarative Pipelines

[Spark Declarative Pipelines](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
can materialise an API into tables and views. They need Spark 4.1 or later.

The Docker image runs the bundled example:

```bash
docker run --rm ghcr.io/neutrinic/apilytics:latest spark-pipelines run --spec /opt/apilytics/examples/sdp/spark-pipeline.yml
```

The spec registers the catalog in its `configuration` block, because the SDP CLI takes no
`--conf` flags:

```yaml
--8<-- "sdp/spark-pipeline.yml"
```

The transformation it runs:

```sql
--8<-- "sdp/transformations/first_pokemon.sql"
```

## Streaming tables

`CREATE STREAMING TABLE ... AS SELECT ... FROM STREAM api.default.<table>` works for
tables configured for [streaming](streaming.md). Pipelines run streams with
`Trigger.AvailableNow`, so **the first run of a new streaming table writes nothing**: it
records where the stream starts, and later runs pick up what changed since.

## Running them on a cluster

Pipelines run on Spark Connect, which needs its Python client (`pyarrow`, `grpcio`,
`grpcio-status`, `googleapis-common-protos` and `zstandard`). The Docker image ships them.

Put the pipeline's `storage` and the warehouse (`spark.sql.warehouse.dir`) on storage
every node can reach, such as HDFS or an object store. With a local `file:` warehouse,
each executor writes to its own disk, and the driver reads materialised views back empty.

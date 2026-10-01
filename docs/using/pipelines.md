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

`spark-pipelines` only runs through Spark Connect. Without `--remote`, it starts its own
Connect server in local mode, so the pipeline never touches your cluster. To run on a
cluster:

1. **Run a Connect server on the cluster** with the APIlytics jar in its Spark config
   (`spark.jars.packages`, as in [Deploying](../deploying/index.md#thrift-and-connect-servers)).
   The server needs only the jar: the catalog itself is registered by the spec.
2. **Point the CLI at it:**

    ```bash
    spark-pipelines --remote sc://connect-host:15002 run --spec spark-pipeline.yml
    ```

3. **Give the CLI a client-only Spark config.** It refuses `--master` and `--deploy-mode`
   (*"Remote cannot be specified with master and/or deploy mode"*), and it refuses them from
   `spark-defaults.conf` too. So on a machine whose `SPARK_CONF_DIR` holds cluster defaults,
   point `SPARK_CONF_DIR` at an empty directory for the CLI.
4. **Put the config file where the Connect server's driver can read it.** The spec's
   `configuration:` keys are sent to the server as session config, so
   `spark.sql.catalog.api.config` is a path on the server's host, not the client's.
5. **Use shared storage.** Put the pipeline's `storage` and the server's warehouse
   (`spark.sql.warehouse.dir`) on storage every node can reach, such as HDFS or an object
   store. With a local `file:` warehouse, each executor writes to its own disk, and the
   driver reads materialised views back empty.

The client needs Spark Connect's Python dependencies (`pyarrow`, `grpcio`,
`grpcio-status`, `googleapis-common-protos` and `zstandard`), plus PyYAML, and pandas below
3.0. The Docker image ships them.

Verified on YARN with Spark 4.1.3 and 4.2.0: a materialised view over PokeAPI and a
streaming table over a live feed, three pipeline runs each, with the streaming table exact.

# Quick start

The Docker image bundles Spark, APIlytics and two example APIs. You don't need Java, Scala
or a build tool:

```bash
docker run -it --rm ghcr.io/neutrinic/apilytics:latest "SELECT name FROM api.default.pokemon LIMIT 20"
```

## Shells

```bash
# Interactive spark-sql shell, with the PokeAPI catalog as `api`
docker run -it --rm ghcr.io/neutrinic/apilytics:latest

# The same, with the bundled GitHub config instead
docker run -it --rm ghcr.io/neutrinic/apilytics:latest --config /opt/apilytics/examples/github/github-config.conf

# Your own config file
docker run -it --rm -v /path/to/my.conf:/config.conf ghcr.io/neutrinic/apilytics:latest --config /config.conf

# Everything the image can do
docker run --rm ghcr.io/neutrinic/apilytics:latest --help
```

The bundled configs:

- `examples/pokeapi/pokeapi-config.conf`: PokeAPI, no authentication. This is the default.
- `examples/github/github-config.conf`: public GitHub repositories, no authentication, limited to 60 requests an hour.

## PySpark

```bash
# Interactive PySpark shell
docker run -it --rm ghcr.io/neutrinic/apilytics:latest pyspark

# Run a Python script
docker run -it --rm ghcr.io/neutrinic/apilytics:latest spark-submit /opt/apilytics/examples/pyspark/basic.py
```

## Jupyter

```bash
docker run -p 8888:8888 --rm ghcr.io/neutrinic/apilytics:latest jupyter
```

Open `http://localhost:8888`; the token is printed in the logs. The `pokeapi` and `github`
catalogs are configured, and example notebooks for Python and Scala are in
`/opt/apilytics/examples/notebooks/`.

## JDBC

```bash
docker run -p 127.0.0.1:10000:10000 --rm ghcr.io/neutrinic/apilytics:latest thrift
```

Connect to `jdbc:hive2://localhost:10000`. The `pokeapi` and `github` catalogs are
configured. The server needs no password, which is why the port is published on loopback
only. See [JDBC and BI tools](../using/jdbc.md).

## Declarative Pipelines

```bash
docker run --rm ghcr.io/neutrinic/apilytics:latest spark-pipelines run --spec /opt/apilytics/examples/sdp/spark-pipeline.yml
```

See [Declarative Pipelines](../using/pipelines.md).

## Next

The image runs Spark in local mode as the non-root user `spark`. To use APIlytics on your
own Spark, see [Installation](installation.md) and [Deploying](../deploying/index.md).

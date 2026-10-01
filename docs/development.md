# Development

How the repository is built and tested. To contribute, see
[CONTRIBUTING.md](https://github.com/Neutrinic/apilytics/blob/main/CONTRIBUTING.md).

## Building

Requires Java 17 or 21, and sbt.

```bash
sbt test       # unit and integration tests
sbt assembly   # target/scala-2.13/apilytics.jar, with its dependencies shaded
```

The build targets Spark 4.2 by default. CI builds and tests the same sources against every
supported Spark release; to try another locally:

```bash
sbt -DsparkVersion=4.0.4 test
```

## A local cluster

`docker/spark` has a standalone cluster: a master, two workers, a history server, a
Thrift server and Jupyter. The assembled jar and `examples/` are mounted into every
container.

```bash
sbt assembly
docker compose -f docker/spark/compose.spark.yaml up -d
```

The helper script builds the jar, starts the cluster and opens a `spark-shell` with an
example API's catalog. The examples are `pokeapi` (the default), `github`, `slack`, and
`multi`, which has GitHub and PokeAPI together:

```bash
./scripts/spark-shell.sh github
```

The Thrift server's catalogs are set in `compose.spark.yaml`:

```yaml
environment:
  - CATALOGS=github,pokemon
  - CATALOG_GITHUB_CONFIG=/opt/spark/examples/github/github-config.conf
  - CATALOG_POKEMON_CONFIG=/opt/spark/examples/pokeapi/pokeapi-config.conf
```

## The Docker image

The published image is built from `docker/dist/Dockerfile`, from the repository root,
after `sbt assembly`:

```bash
sbt assembly
docker build -t apilytics:latest -f docker/dist/Dockerfile .
```

## This site

The site is built with [Zensical](https://zensical.org/) from `docs/` and `zensical.toml`:

```bash
pip install zensical==0.0.67
zensical serve
```

It's published per release, one version per minor release, with a version selector. Pull
requests that touch the docs build them in strict mode.

## Architecture

```
Config (HOCON) → OpenAPI parser → Schema mapper → Arrow schema
                                                        ↓
Spark catalog ← Tables ← ScanBuilder (pushdown) → HTTP client
                                                        ↓
                                              Paginator (fs2 Stream)
                                                        ↓
                                              Arrow converter → ColumnarBatch
```

## Stack

- Scala 2.13, built against Spark 4.0 to 4.2
- http4s Ember client, on cats-effect and fs2
- circe, with circe-pointer for JSON pointers (RFC 6901)
- swagger-parser, for OpenAPI
- Typesafe Config, for HOCON
- Apache Arrow, the one Spark ships, for columnar batches

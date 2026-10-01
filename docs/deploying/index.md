# Deploying

Every page in this section describes a deployment that was run for real. Each one ran the
same check:

- a read from a public API
- a 400-row batch read split over 4 partitions
- a 20-minute streaming run across a restart, checked row by row against the source, with
  no rows missing and none duplicated

| Platform | Spark | Entry point | Jar from | Verified |
|---|---|---|---|---|
| [Standalone](standalone.md) | 4.0.4, 4.1.3, 4.2.0 | catalog, `format()` | `--packages` | ✅ |
| [YARN](yarn.md) | 4.0.4, 4.1.3, 4.2.0 | catalog, `format()` | `--packages` | ✅ |
| [Kubernetes](kubernetes.md), `spark-submit` | 4.0.4, 4.1.3, 4.2.0 | catalog, `format()` | `--packages` | ✅ |
| [Kubernetes](kubernetes.md#spark-operator), Spark Operator | 4.2.0 | catalog, `format()` | `deps.packages` | ✅ |
| [Databricks](databricks.md), AWS and Azure | DBR 17.3 (4.0), DBR 18 (4.1) | `format()` only | a Unity Catalog volume | ✅ |
| [EMR on EC2](emr.md) | 4.0.2-amzn | catalog | `--jars s3://` | ✅ |
| [EMR Serverless](emr.md#emr-serverless) | | | `--jars s3://` | ⚠️ needs a VPC with NAT |
| [Dataproc](dataproc.md), Compute Engine | 4.1.2 | catalog | `--jars gs://` | ✅ |
| [Dataproc Serverless](dataproc.md#dataproc-serverless) | 4.0.2 | catalog | `--jars gs://` | ✅ |

`format()` is the same table as the catalog, so where only the catalog was run, `format()`
is expected to behave the same; it was run on the lab platforms and on Databricks.
Platforms that can't run APIlytics are listed under [Unsupported](unsupported.md).

## The job

The recipes in this section submit `your-job.py`. Any Spark job works. A minimal one,
with the catalog registered on the command line:

```python
--8<-- "job.py"
```

## What every deployment needs

**The jar, on the driver.** Executors get it from the driver, so it's only named on the
submit command: `--packages io.github.neutrinic:apilytics_2.13:<version>` from Maven
Central, or `--jars` with the jar's path. The jar is self-contained, so it doesn't clash
with the libraries a platform ships.

**The config, as a local file on the driver.** The catalog's `config` setting is a path
on the driver's own filesystem, not HDFS or a URL. Executors never read it: their tasks
carry the resolved settings. A spec named relatively in `openapi` is found beside the
config, so copy the two together.

**Outbound internet, from the driver and every executor.** Executors call the API. A
cluster in a private network needs a NAT gateway or a proxy. Without one, every request
fails after `http.timeout` with `No response from <host>`.

**Shared storage for streaming checkpoints.** A stream's `checkpointLocation` must survive
the driver. Use HDFS or an object store, or a Databricks volume, rather than the driver's
local disk.

## Thrift and Connect servers

A Thrift or Connect server serves the catalog to its clients. Set the jar and the catalog
in the server's own Spark config, and clients query `api.default.<table>` as usual:

```properties
--8<-- "server-defaults.conf"
```

Verified on standalone with beeline over JDBC, and with a Python Connect client, including
a pushed-down filter. [Declarative Pipelines](../using/pipelines.md) run through a Connect
server the same way.

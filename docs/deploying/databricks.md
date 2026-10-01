# Databricks

Verified on Databricks on AWS and on Azure Databricks, with dedicated (single-user) access
mode, on DBR 17.3 LTS (Spark 4.0, Java 17) and DBR 18 (Spark 4.1, Java 21):

| | Public API | Batch | Streaming, checkpoint in a volume |
|---|---|---|---|
| AWS, DBR 17.3 | PASS | 400/400 | 1218/1218 |
| AWS, DBR 18 | PASS | 400/400 | 1200/1200 |
| Azure, DBR 17.3 | PASS | 400/400 | 1210/1210 |
| Azure, DBR 18 | PASS | 400/400 | 1203/1203 |

## Use `format()`, not the catalog

On Unity Catalog compute, Databricks routes every catalog name to Unity Catalog, so a
`spark.sql.catalog.*` plugin is never loaded. Read the same tables through the data
source instead:

```python
issues = (spark.read.format("apilytics")
          .option("config", "/Volumes/main/default/apilytics/api.conf")
          .option("table", "issues")
          .load())
```

```sql
CREATE TEMPORARY VIEW issues USING apilytics
OPTIONS (config '/Volumes/main/default/apilytics/api.conf', table 'issues');
```

Streaming works the same way, with `spark.readStream.format("apilytics")`. See
[Installation](../getting-started/installation.md#without-a-catalog-formatapilytics) for
what `format()` gives up.

## Setup

1. **Upload the jar to a Unity Catalog volume** and attach it to the cluster as a library:
   `{"jar": "/Volumes/<catalog>/<schema>/<volume>/apilytics.jar"}`. Installing by Maven
   coordinate should work the same way, but hasn't been run yet.
2. **Put the config and spec where the driver can read them as files.** Volumes
   (`/Volumes/...`) and workspace files (`/Workspace/...`) both appear to the driver as
   local paths. The verified runs wrote the config to the driver's local disk at run time;
   a volume path is read the same way.
3. **Put streaming checkpoints in a volume.**
4. **Run the job's own Python file from workspace files.** A `spark_python_task` can't read
   its Python file from a volume: it fails with *"Cannot read the python file
   /Volumes/…"*.

## Limits

- **Serverless compute** doesn't support custom jars, so it can't run APIlytics.
- **Standard (shared) access mode** hasn't been run. It needs a metastore admin to
  allowlist the jar and to grant `ANY FILE`.
- **Clusters without Unity Catalog** could use the catalog, but new workspaces block them
  by default.
- **DBR 18 ships its own repackaged copy of cats**, inside a bundled notebook kernel's
  jar, which breaks an unshaded http4s. The APIlytics jar shades its dependencies, so the
  two don't meet. A library clash of that kind would now fail at once with a clear
  message, rather than hang.
- **Azure core quota:** a new subscription's regional limit is 10 cores. Two
  4-core-per-node, 2-node clusters at once fail with `AZURE_QUOTA_EXCEEDED_EXCEPTION`, so
  size the cluster within it, or raise the quota.

Databricks on GCP runs the same runtime builds, so it should behave the same, but it
hasn't been run. Check that its clusters can reach the API.

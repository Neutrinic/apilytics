# EMR

## EMR on EC2

Verified on release `emr-spark-8.0.0` (Spark 4.0.2-amzn, Java 17), in cluster and client
deploy modes, through the catalog:

| Deploy mode | Jar and job from | Streaming checkpoint | Streaming |
|---|---|---|---|
| cluster | `s3a://` | `s3a://` | 1213/1213 |
| client | `s3://` | `s3a://` | 1210/1210 |

Run the job as a Spark step, with the jar from S3. The config and spec ship with
`--files`, and the catalog names the config by its bare file name:

```json
[{"Type": "Spark", "Name": "apilytics-job", "ActionOnFailure": "CONTINUE",
  "Args": ["--deploy-mode", "cluster",
           "--jars", "s3://your-bucket/apilytics.jar",
           "--files", "s3://your-bucket/api.conf,s3://your-bucket/api-spec.yaml",
           "--conf", "spark.sql.catalog.api=com.apilytics.spark.RESTCatalog",
           "--conf", "spark.sql.catalog.api.config=api.conf",
           "s3://your-bucket/your-job.py"]}]
```

- **Jar:** `--jars` with an `s3://` or `s3a://` path both worked. `--packages` from Maven
  Central should work too, given internet access from the primary node, but it hasn't
  been run on EMR.
- **Config:** EMR runs Spark on YARN, so in cluster mode the config and spec ship with
  `--files`, as above. See [YARN](yarn.md). In client mode the driver runs on the primary
  node, where a local path works. The verified runs wrote their config on the driver at
  run time, so the `--files` route is the expected setup rather than a verified one.
- **Checkpoints:** EMR 8 doesn't ship EMRFS, but both `s3://` and `s3a://` paths worked.
- **Cost:** create the cluster with auto-termination after the last step, so a test
  cluster doesn't keep running.

## EMR Serverless

**An EMR Serverless application has no internet access unless it runs in a VPC with a NAT
gateway.** Without one, it can't reach any API outside AWS. Create the application with a
network configuration: private subnets whose route table sends outbound traffic through
a NAT gateway, and a security group that allows outbound HTTPS.

Without that, every request fails after `http.timeout`. Before 1.0.0, a request waited for
the operating system's TCP connect timeout instead, which made a job hang until its own
timeout.

The job itself is submitted like any EMR Serverless Spark job:

```json
{"sparkSubmit": {
   "entryPoint": "s3://your-bucket/your-job.py",
   "sparkSubmitParameters": "--jars s3://your-bucket/apilytics.jar --files s3://your-bucket/api.conf,s3://your-bucket/api-spec.yaml --conf spark.sql.catalog.api=com.apilytics.spark.RESTCatalog --conf spark.sql.catalog.api.config=api.conf"}}
```

A full run through a NAT-enabled application hasn't been done, so treat this as the
expected setup rather than a verified one.

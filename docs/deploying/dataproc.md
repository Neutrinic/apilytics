# Dataproc

## Dataproc on Compute Engine

Verified on image `3.0.2-debian13` (Spark 4.1.2, Scala 2.13, Java 21), on a cluster with
internal IPs only and outbound internet through Cloud NAT, through the catalog:

| Deploy mode | Public API | Batch | Streaming, checkpoint on GCS |
|---|---|---|---|
| client (the jobs API default; driver on the master) | PASS | PASS | 1195/1195 |
| cluster (`spark.submit.deployMode=cluster`) | PASS | PASS | 1195/1195 |

```bash
gcloud dataproc jobs submit pyspark gs://your-bucket/your-job.py \
  --cluster your-cluster --region your-region \
  --jars gs://your-bucket/apilytics.jar \
  --properties spark.sql.catalog.api=com.apilytics.spark.RESTCatalog,spark.sql.catalog.api.config=/path/on/driver/api.conf
```

The catalog can also be registered at run time with `spark.conf.set`; both worked.

The config has to be a local file on the driver. In client mode, the driver runs on the
master node. In cluster mode (`spark.submit.deployMode=cluster`), it runs on a worker, so
ship the config and spec with `--files gs://.../api.conf,gs://.../api-spec.yaml` and name
the config by its bare file name, as on [YARN](yarn.md). The verified runs wrote the config
on the driver at run time, so the `--files` route is the expected setup rather than a
verified one.

- **Internal-IP clusters need Cloud NAT** for executors to reach the API. Private Google
  Access covers only Google's own APIs, such as GCS.
- **The default 1 TB disk per node** exceeds a new project's 2 TB `DISKS_TOTAL_GB` quota at
  three nodes. Pass `--master-boot-disk-size` and `--worker-boot-disk-size`; 100 GB is
  plenty.
- **The cluster's service account needs `roles/dataproc.worker`.**

## Dataproc Serverless

Verified on runtime 3.0 (Spark 4.0.2, Scala 2.13, Java 21), through the catalog: public
API PASS, batch 400/400, streaming 1204/1204 across a restart with its checkpoint on GCS.

```bash
gcloud dataproc batches submit pyspark gs://your-bucket/your-job.py \
  --region your-region --version=3.0 --subnet your-subnet \
  --jars gs://your-bucket/apilytics.jar \
  --user-workload-authentication-type=SERVICE_ACCOUNT --service-account your-sa@your-project.iam.gserviceaccount.com
```

- **Config:** a batch's driver is created for the batch, so ship the config and spec with
  `--files` and name the config by its bare file name. As on Compute Engine, the verified
  run wrote its config at run time instead.
- **Enable the Cloud Resource Manager API** in the project. Without it, batches fail before
  starting with `TagKeys.GetNamespacedTagKey PERMISSION_DENIED`.
- **Runtime 3.0 runs batches with end-user credentials by default.** Pass both flags above
  to run as a service account, or grant the one-time OAuth consent Dataproc links to.
- **Executors need Cloud NAT** on the batch's subnet to reach the API, as on Compute Engine.
- **Capacity:** a region can refuse a batch with *"does not have enough resources"*.
  Retrying later worked.

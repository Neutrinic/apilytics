# Kubernetes

Verified on k3s with Spark 4.0.4, 4.1.3 and 4.2.0 through `spark-submit` in cluster
deploy mode, and with the Kubeflow Spark Operator 2.5.2 on Spark 4.2.0. Both the catalog
and `format()` were run. Executors ran on all three nodes, and streaming was exact.

## `spark-submit`

```bash
--8<-- "deploy-k8s.sh"
```

The [YARN recipe](yarn.md) doesn't carry over: `--files` and local Maven repositories are
local to the machine you submit from. Kubernetes refuses local files unless
`spark.kubernetes.file.upload.path` points at shared storage. What works:

- **Jar:** `--packages` is resolved **by the driver pod**, from a Maven repository it can
  reach over HTTP: Maven Central, or your mirror. Point `spark.jars.ivy` at a writable
  path such as `/tmp/ivy`. Executors get the jar from the driver, and nothing needs
  uploading.
- **Config and spec:** they must be **inside the driver pod**, and the catalog's `config`
  is the path inside the container. The verified runs mounted them with a hostPath volume,
  as above, which needs the files on the node the driver is placed on. In production, put
  the same files at the same path by baking them into your image, or by mounting a
  ConfigMap through a driver pod template. If the mount renames the files, set `openapi`
  to the spec's absolute path inside the pod, or a URL, because a relative spec is looked
  up beside the config under its original name.
- **Streaming checkpoints:** a checkpoint on the driver pod's disk lasts only as long as
  that pod. A stream that has to survive a restart needs object storage or a persistent
  volume.

## Spark Operator

The same job as a `SparkApplication`, for the
[Kubeflow Spark Operator](https://github.com/kubeflow/spark-operator):

```yaml
--8<-- "sparkapplication.yaml"
```

- **Declare placement and volumes in `sparkConf`.** The operator's own fields for them,
  such as `spec.driver.volumeMounts` and `nodeSelector`, only take effect when its
  mutating webhook is enabled. A run that relied on them under an operator without the
  webhook failed: the driver couldn't find its files. `sparkConf` entries are applied by
  Spark itself, so they work either way.
- **Use a service account that can create executor pods** in the namespace, set with
  `spec.driver.serviceAccount`.
- `deps.packages` and `deps.repositories` are `--packages` and `--repositories`. The
  driver pod resolves them, as above.

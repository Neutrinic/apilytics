# Standalone

Verified on Spark 4.0.4, 4.1.3 and 4.2.0, on a three-node cluster, in client deploy mode
with the driver on the master host. Both the catalog and `format()` were run. Partitions
were read on all three hosts, and streaming was exact across a stop and restart.

```bash
--8<-- "deploy-standalone.sh"
```

- **Jar:** `--packages` resolves it from Maven Central where you submit. Or use
  `--jars /path/to/apilytics.jar`. Either way it's named only on the submit command;
  executors get it from the driver.
- **Config and spec:** they only need to exist on the driver's host. The config path is a
  local path there. Executors never read either file.
- **Streaming checkpoints:** a `file://` checkpoint on the driver's host works, because
  the driver writes the offsets and commits, and APIlytics keeps no stream state on
  executors. It's only as durable as that host's disk, so use HDFS or an object store for
  a stream that has to survive the host.

In cluster deploy mode the driver runs on a worker. The config then has to be at the same
path on whichever worker that is, or be distributed with `--files`, as on
[YARN](yarn.md).

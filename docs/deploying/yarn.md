# YARN

Verified on Spark 4.0.4, 4.1.3 and 4.2.0, in **cluster** deploy mode, so the driver ran in
a YARN container on a different node each time. Both the catalog and `format()` were run.
Partitioning was exact, and streaming was exact across a stop and restart with its
checkpoint on HDFS.

```bash
--8<-- "deploy-yarn.sh"
```

- **Jar:** `--packages` resolves it where you submit and ships it through YARN's staging
  directory, so the cluster nodes need no Maven access.
- **Config and spec:** ship both with `--files`, and set the catalog's `config` to the
  config's **bare file name**. YARN copies `--files` into the driver container's working
  directory, and the spec resolves beside the config. This is the one step you can't
  guess: an absolute path only works if the file happens to exist on whichever node YARN
  picks for the driver.
- **Why not HDFS:** on YARN, an unqualified path means HDFS to Spark, but the catalog's
  config is always read from the local filesystem. That's why the recipe is `--files` plus
  a relative name.
- **Streaming checkpoints:** use HDFS, for example `hdfs:///checkpoints/<stream>`. It
  survives the driver restarting on another node, which a local path wouldn't.

In client deploy mode, the driver runs where you submit, so a local path to the config
works as on [standalone](standalone.md).

EMR runs Spark on YARN, so this recipe applies there too. See [EMR](emr.md).

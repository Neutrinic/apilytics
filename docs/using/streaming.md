# Streaming

Read an endpoint as a micro-batch source:

```scala
spark.readStream.table("api.default.issues")
  .writeStream.format("console").start()
```

Or without a catalog: `spark.readStream.format("apilytics").option("config", ...).option("table", "issues").load()`.

## Which tables can stream

A table streams only with timestamp checkpointing, because each batch asks the API for
what changed:

```hocon
tables.issues {
  endpoint = "/issues"
  checkpoint {
    mode            = timestamp
    timestamp-param = "since"        # query parameter the API filters on
    timestamp-path  = "/updated_at"  # where the timestamp lives in a record
  }
}
```

Both keys are required: the parameter asks the API for a window, and the path bounds it.
Tables without them are batch-only, and a streaming read of one is rejected at analysis
rather than at run time.

## What a stream delivers

- **A new stream starts from now.** It delivers only new records, so use a batch query to
  load history.
- **The first `AvailableNow` run writes nothing.** This is the one to know about before
  you think it's broken. With `Trigger.AvailableNow`, which Declarative Pipelines uses,
  the first run spans `[now, now]`. That run records the offset, and every later run picks
  up what changed since.
- **Delivery is at least once.** A record that becomes visible to the API after the batch
  covering its timestamp has run is missed, so an API with delayed visibility needs a lag
  applied at the source.
- **Batch windows are `(start, end]`, inclusive at the end.** That suits the usual
  exclusive `since`. If your API's `since` is inclusive, expect one duplicate per batch
  boundary rather than a gap.
- **Timestamps are compared as instants**, so mixed precision between the offset and the
  API's own values is handled. ISO-8601 with `Z` or an offset is understood. Epoch seconds
  and other formats aren't, and records with them are kept rather than dropped.
- **Each batch ends at the last fully elapsed second**, so a stream runs up to a second
  behind the clock. That's what makes second-precision APIs safe: a batch never closes a
  second that records are still being stamped with.

The query's own checkpoint (`checkpointLocation`) holds the offsets, as for any Spark
stream. Put it on storage every node can reach.

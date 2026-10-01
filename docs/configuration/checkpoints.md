# Checkpoints

A checkpoint makes repeated batch reads resume where the last one stopped. On the first
run, all the data is fetched, and the final pagination state is saved. Later runs resume
from that state.

!!! warning "A resume point, not a delivery guarantee"
    The state is saved when a read ends, **including when it fails or is cancelled**. It
    tracks what was fetched, not what Spark successfully wrote downstream. So a failed run
    can move the checkpoint past records that were never delivered. In `cursor` mode,
    the saved cursor is the one that fetched the last page, so the next run reads that
    page again. Tracked in [#280](https://github.com/Neutrinic/apilytics/issues/280).

    [Streaming](../using/streaming.md) tracks progress more carefully. Spark keeps the
    stream's offsets itself, and commits a batch only after the sink has finished it. That
    tracks the *source*, though. Whether the output can hold duplicates depends on the
    sink: a batch replayed after a failure is written again, so a sink that is neither
    transactional nor idempotent, such as Kafka or a plain `foreachBatch`, can receive it
    twice. Deduplicate by batch ID, or write idempotently.

```hocon
tables {
  events {
    endpoint = "/events"
    checkpoint {
      enabled = true
      path = "/tmp/apilytics/checkpoints"  # a local path, or s3a://, gs://, hdfs://
      mode = "cursor"                      # cursor | offset | timestamp
    }
  }
}
```

| `mode` | Resumes from | Saves |
|---|---|---|
| `cursor` | The last pagination cursor | The last cursor value |
| `offset` | The last numeric offset | The next offset |
| `timestamp` | A record timestamp, sent as a query parameter | The latest record timestamp |

Timestamp mode also needs to know which field to track and which parameter to send.
`checkpoint` always belongs to a table:

```hocon
tables {
  events {
    endpoint = "/events"
    checkpoint {
      enabled = true
      path = "s3a://my-bucket/checkpoints"
      mode = "timestamp"
      timestamp-path = "/updated_at"    # JSON pointer to the timestamp in each record
      timestamp-param = "since"         # query parameter to filter on
    }
  }
}
```

Each table's checkpoint is stored as `<table-name>.checkpoint.json` under `path`. Local
paths are written directly. Remote paths (`hdfs://`, `s3://`, `s3a://`, `gs://`) go
through Hadoop's FileSystem, so the cluster needs the matching connector.

Timestamp mode is also what lets a table [stream](../using/streaming.md). A streaming
query keeps its offsets in its own `checkpointLocation`, not in this file.

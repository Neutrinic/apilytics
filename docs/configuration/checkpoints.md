# Checkpoints

A checkpoint makes repeated batch reads resume where the last one stopped. On the first
run, all the data is fetched, and the final pagination state is saved. Later runs resume
from that state.

**The checkpoint moves when the Spark task that read the table succeeds.** A reading task
that fails partway, is killed, or fails later in its own work (such as writing its
output in the same task) leaves the checkpoint where it was, so the next run reads those
records again instead of skipping them.

Some records can be read twice:

- **After a failed reading task**, the next run starts from the previous checkpoint, so
  records the failed task had already delivered come again.
- **In `cursor` mode**, the API's last page has no next cursor. The saved cursor is the one
  that fetched that page, so the next run reads it again, along with anything appended
  to it since. That's what keeps records added to the last page from being missed.

!!! warning "Not an end-to-end delivery guarantee"
    The checkpoint follows the *reading* task, not the whole query. If the query goes on
    past that task (after a shuffle, in a later stage, or in a sink's final commit) and
    fails there, the reading task has already succeeded and moved the checkpoint.
    Rerunning the query then starts after records that never reached the output, and
    deduplicating downstream can't bring them back. Use batch checkpoints where the read
    and the write happen in the same task, or where re-reading from an earlier point is
    possible; for end-to-end tracking, use streaming.

!!! note "Batch checkpoints and streaming"
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

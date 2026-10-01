# Checkpoints

A checkpoint makes repeated batch reads incremental. On the first run, all the data is
fetched, and the final pagination state is saved. Later runs resume from that state and
fetch only new data.

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

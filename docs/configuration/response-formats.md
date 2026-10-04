# Response formats

By default APIlytics expects full-body JSON responses. For APIs that stream their
responses, set `http.response-format`:

| `response-format` | Content type | Reads |
|---|---|---|
| `json` | `application/json` | The default: one JSON body per page. |
| `ndjson` | `application/x-ndjson` | Newline-delimited JSON (JSON Lines), one record per line. |
| `sse` | `text/event-stream` | Server-Sent Events, parsing each `data:` field as JSON. |

```hocon
http {
  response-format = "ndjson"
}
```

- **NDJSON** suits exports and change feeds, such as a BigQuery export, Elasticsearch's
  scroll API or CouchDB's changes feed.
- **SSE** suits real-time feeds and change-data-capture streams.

These formats bypass pagination, because they represent one continuous stream. Use
`LIMIT` in SQL to cap the number of records read.

With `ndjson`, strict mode takes its columns from the record the spec describes under the
endpoint's NDJSON response (`application/x-ndjson`, `application/jsonl` and similar): an
object, or an array whose items are the records. If the spec gives no record there, for
example just `type: string`, use variant mode instead. SSE streams are read in variant mode:
specs describe their events too differently to take columns from.

Working examples:
[`examples/lichess/lichess-ndjson.conf`](https://github.com/Neutrinic/apilytics/blob/main/examples/lichess/lichess-ndjson.conf)
and
[`examples/sse/sse-demo.conf`](https://github.com/Neutrinic/apilytics/blob/main/examples/sse/sse-demo.conf).

These are about how a response is encoded. To read an API as a Spark stream, see
[Streaming](../using/streaming.md).

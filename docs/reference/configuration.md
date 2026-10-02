# Configuration reference

Every key a config file accepts, with its default. A key not listed here is rejected when
the config loads, so a typo fails loudly instead of being silently ignored.

Durations are written as `"30 seconds"`, `"30s"`, `"5 minutes"` or `"7 days"`.

## Registering a config

| Setting | Meaning |
|---|---|
| `spark.sql.catalog.<name>` | `com.apilytics.spark.RESTCatalog`, registering a catalog called `<name>`. |
| `spark.sql.catalog.<name>.config` | The config file's path on the driver. |
| `format("apilytics")` option `config` | The same path, for the [data-source entry point](../getting-started/installation.md#without-a-catalog-formatapilytics). |
| `format("apilytics")` option `table` | The table to read, as named under `tables`. |

The file is read on the driver only. Executors receive the resolved settings with their
tasks.

## Top level

| Key | Default | Meaning |
|---|---|---|
| `openapi` | required | The OpenAPI spec: a URL, an absolute path, or a path relative to the config file's directory. |
| `base-url` | the spec's first server | The API's base URL, overriding the spec's `servers`. |
| `auth` | required | [Authentication](#auth). |
| `pagination` | `style = none` | [Pagination](#pagination) for every table, unless a table sets its own. |
| `schema` | | [Schema](#schema). |
| `http` | | [HTTP](#http). |
| `tables` | | [Tables](#tables), keyed by table name. |
| `cache` | disabled | [Spec cache](#cache). |

## `auth`

| Key | Default | Meaning |
|---|---|---|
| `type` | required | `none`, `bearer`, `basic`, `header` or `oauth2_client`. |
| `token` | | For `bearer`. |
| `username`, `password` | | For `basic`. |
| `header-name`, `header-value` | | For `header`. |
| `client-id`, `client-secret`, `token-url` | | For `oauth2_client`: the client-credentials flow. All three are required unless a pre-fetched `token` is given instead. |

Credentials sent over plain `http://`, in `base-url` or `token-url`, are logged as a
security warning. See [Credentials](../configuration/credentials.md).

## `pagination`

| Key | Default | Meaning |
|---|---|---|
| `style` | `none` | `none`, `offset`, `cursor` or `link_header`. |
| `offset-param` | `offset` | For `offset`: the query parameter holding the start offset. |
| `page-size-param` | `limit` for `offset`; otherwise not sent | The query parameter holding the page size. For `cursor` and `link_header`, no page size is sent unless this is set. |
| `max-page-size` | `100` | The page size requested. A pushed-down `LIMIT` smaller than this is requested instead. |
| `results-path` | | For `offset`: a JSON pointer to the page's record array, used to recognise an empty last page. Without it, an empty top-level array ends the walk. |
| `cursor-path` | required for `cursor` | A JSON pointer to the next cursor in the response. The walk ends when it's missing or empty. |
| `cursor-param` | `cursor` | For `cursor`: the query parameter that sends the next cursor. |
| `max-pages` | `1000` | A safety limit on pages per walk, against an API that never stops. |

`link_header` follows the `Link: rel="next"` response header. A [checkpoint](#checkpoint)
on a `link_header` source must use `mode = timestamp`, because the header carries no
cursor or offset state to save.

## `schema`

| Key | Default | Meaning |
|---|---|---|
| `mode` | `strict` | `strict` (typed columns) or `variant` (one VARIANT column). See [Schema](../configuration/schema.md). |
| `flatten-depth` | `2` | Levels of nested objects flattened into columns. Deeper objects become JSON strings. |
| `array-handling` | `keep_array` | `keep_array`, `explode_view` or `both`. |
| `explode-outer` | `false` | In exploded tables, keep a row for a record whose array is empty or null. |
| `arrow-batch-size` | `4096` | Rows per Arrow batch handed to Spark. |
| `prefetch-batches` | `2` | Batches fetched ahead of Spark, per partition. At least 1. |

## `http`

| Key | Default | Meaning |
|---|---|---|
| `timeout` | `30 seconds` | Per request: connecting, sending, and receiving the response headers. |
| `max-retries` | `5` | Retries for transient failures (429 and 5xx), with exponential backoff. |
| `max-backoff` | `30 seconds` | The longest wait between retries. |
| `rate-limit` | none | Requests per second, shared between partitions. See [Rate limiting](../using/rate-limiting.md). |
| `response-format` | `json` | `json`, `ndjson` (also `jsonl`) or `sse`. See [Response formats](../configuration/response-formats.md). |
| `response-cache` | disabled | [Response cache](#response-cache). |

### `response-cache`

An in-memory cache of API responses, shared by the tasks in each JVM (the driver, and
each executor). Useful when the same pages are read repeatedly within its TTL.

Entries are keyed by host, credentials, path and query parameters, so catalogs in the same
application never share responses, even when they request the same path. The credentials
are hashed rather than stored.

A query that mixes cached and fresh pages isn't a consistent snapshot of the API: pages
cached at different times can overlap or miss records that moved between pages.

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | |
| `backend` | `memory` | The only backend. |
| `ttl` | `5 minutes` | How long a response is reused. |
| `max-entries` | `1000` | Responses kept at most. |

## `cache`

A cache of the parsed OpenAPI spec on local disk, so a large spec isn't fetched and parsed
on every catalog load.

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | |
| `ttl` | indefinite | How long a cached spec is trusted. Without a TTL, it's revalidated by ETag or file modification time. |
| `directory` | `~/.apilytics/cache/` | Where cached specs are kept. |

## `tables`

Each key under `tables` names a table.

| Key | Default | Meaning |
|---|---|---|
| `endpoint` | required | The path, relative to the base URL. `{name}` placeholders are filled from a parent table. |
| `data-path` | | A JSON pointer to the records, when the response wraps them. |
| `pagination` | the source's | This table's own [pagination](#pagination). |
| `filters` | none | [Filter pushdown](#filters). |
| `partition` | none | [Partitioning](#partition). |
| `parent-table`, `parent-key`, `join-strategy` | | [Parent-child joins](#parent-child-joins). |
| `aggregations` | none | [Aggregate pushdown](#aggregations). |
| `count` | none | Deprecated [`COUNT(*)` pushdown](#count-deprecated). |
| `checkpoint` | none | [Checkpoints](#checkpoint). |

### `filters`

A list of predicates that become query parameters:

```hocon
filters = [
  { param = "state", column = "state", operators = ["eq"] }
  { param = "since", column = "updated_at", operators = ["gt", "gte"] }
]
```

| Key | Meaning |
|---|---|
| `column` | The column the SQL predicate is on. |
| `operators` | Which predicates push: `eq` (`=`), `neq` (`<>`), `gt` (`>`), `gte` (`>=`), `lt` (`<`), `lte` (`<=`). |
| `param` | The query parameter the value is sent in. |

A predicate without a matching entry is applied by Spark after the rows arrive. See
[Troubleshooting](../troubleshooting.md#which-filters-reach-the-api).

### `partition`

| Key | Default | Meaning |
|---|---|---|
| `type` | `date-range` | `offset`, `enum` or `date-range`. |
| `size`, `count` | required for `offset` | Partition `i` covers offsets `[i * size, (i + 1) * size)`. Both at least 1. |
| `param`, `values` | required for `enum` | One partition per value, sent in `param`. |
| `column`, `range`, `start-param`, `end-param` | required for `date-range` | Splits the pushed-down window on `column` into chunks of `range`, which must be at least 1 millisecond. |
| `format` | `yyyy-MM-dd'T'HH:mm:ss'Z'` | For `date-range`: how the bounds are written. |

See [Partitioning](../using/partitioning.md) for which pagination each type works with.

### Parent-child joins

| Key | Default | Meaning |
|---|---|---|
| `parent-table` | | The table whose rows drive the calls. |
| `parent-key` | | The parent column substituted into the endpoint's placeholder. |
| `join-strategy` | `nested_loop` | `nested_loop` (one call per parent row) or `batch` (many parent keys per call). |
| `batch-param` | required for `batch` | The query parameter carrying the batched keys. The endpoint can't have placeholders, so the parent key column is named after `parent-key`. |
| `batch-size` | `100` | Parent keys per call. |
| `batch-separator` | `,` | How the keys are joined. |
| `child-key-field` | derived from `parent-key` | For `batch`: the field in each child record that holds its parent's key, used to match results back to parents. Without it, common names derived from `parent-key` are tried. |

See [Parent-child joins](../using/joins.md).

### `aggregations`

Aggregates that push down to an endpoint which computes them, keyed by a name you choose:

```hocon
aggregations {
  total_issues { function = "count", endpoint = "/search/issues", response-path = "/total_count"
                 params { q = "repo:owner/repo" } }
}
```

| Key | Meaning |
|---|---|
| `function` | `count`, `sum`, `avg`, `min`, `max` or `custom`. |
| `column` | Required for `sum`, `avg`, `min` and `max`. |
| `name` | Required for `custom`: the SQL function's name. |
| `endpoint` | The endpoint that returns the aggregate. |
| `response-path` | A JSON pointer to the value in its response. |
| `params` | Query parameters to send, as `name = value` pairs. |

Only `count`, `sum` and `avg` are pushed down. `min`, `max` and `custom` are accepted, but
pushdown declines them at plan time and Spark computes them over a full scan, because
their result type can't be decided before the data arrives. The decision is logged at
INFO.

### `checkpoint`

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | |
| `path` | required when enabled | The directory for `<table>.checkpoint.json`: a local path, or `hdfs://`, `s3://`, `s3a://`, `gs://`. |
| `mode` | `cursor` | `cursor`, `offset` or `timestamp`. |
| `timestamp-path` | required for `timestamp` | A JSON pointer to each record's timestamp. |
| `timestamp-param` | required for `timestamp` | The query parameter that sends the last timestamp. |

`mode = timestamp`, with both of its keys, is also what lets a table
[stream](../using/streaming.md). See [Checkpoints](../configuration/checkpoints.md).

### `count` (deprecated)

`COUNT(*)` pushdown from before `aggregations`. It still works; a `count` aggregation does
the same.

| Key | Default | Meaning |
|---|---|---|
| `endpoint` | | A dedicated count endpoint, such as `/items/count`. |
| `param` | | Or: a query parameter that makes the normal endpoint include a total. |
| `param-value` | `true` | The value sent in `param`. |
| `response-path` | required | A JSON pointer to the count in the response. |

One of `endpoint` or `param` is required. The equivalent aggregation:

```hocon
# Before
count { endpoint = "/items/count", response-path = "/total" }

# After
aggregations { total { function = "count", endpoint = "/items/count", response-path = "/total" } }
```

# Partitioning

Partitioning splits a table across executors so they read it in parallel. There are three
strategies, and which one fits depends on what the endpoint lets you split on.

## Offset

For an endpoint that offers nothing to split on but its rows. Partition `i` covers
`[i * size, (i + 1) * size)`:

```hocon
tables.pokemon {
  endpoint  = "/api/v2/pokemon"
  data-path = "/results"
  partition { type = "offset", size = 400, count = 4 }
}
```

It needs `pagination.style = offset` and the default `json` response format. Cursor and
link-header pagination take the next page from the response, `none` fetches a single page,
and `ndjson` and `sse` bypass pagination. Under any of those, a start offset means nothing,
and every partition would read the same rows. Configs that ask for it anyway are rejected
at load.

`size × count` should cover the endpoint. Partitions past the end return nothing, and rows
beyond it aren't read, so an endpoint that has grown since the config was written is
truncated rather than duplicated.

Each window stops exactly at its boundary, whatever its size relative to the page size:
the last request asks only for the records the window still needs, and the offset
advances by the records each page actually held. An API that sends more records than
asked for has the page trimmed to what was asked for, as long as it can be counted: a
top-level array, or the array at `results-path`. An API that sends fewer is fine: the
window keeps reading until it's full.

## Enum

When a query parameter splits the data naturally:

```hocon
partition { type = "enum", param = "kind", values = ["fire", "water", "grass"] }
```

A query whose filter is already pushed to the same parameter, such as `WHERE kind = 'fire'`
with a `filters` entry for `kind`, reads a single partition with that value. The filter
has already chosen it.

## Date range

Splits a time window into chunks. The window comes from the query: both of its bounds
have to be pushed down to the API, through filters on `start-param` and `end-param`.

```hocon
tables.events {
  endpoint = "/events"
  filters = [
    { param = "since", column = "created_at", operators = ["gte"] }
    { param = "until", column = "created_at", operators = ["lt"] }
  ]
  partition { type = "date-range", column = "created_at", range = "7 days"
              start-param = "since", end-param = "until" }
}
```

Here `created_at` is a string column holding ISO-8601 timestamps, so the literals in the
query are sent as written:

```sql
-- Four partitions, one per week
SELECT * FROM api.default.events
WHERE created_at >= '2026-01-01T00:00:00Z' AND created_at < '2026-01-29T00:00:00Z';
```

If either bound isn't pushed down, because the query doesn't filter on it or no filter
maps it to its parameter, the read falls back to a single partition and logs a warning.
The bounds are written in `format`, `yyyy-MM-dd'T'HH:mm:ss'Z'` by default, so a pushed
value has to parse in that format.

## Parameters pagination already uses

Enum and date-range partitioning can't use a parameter that pagination already controls.
The paginator would overwrite it, and every partition would read the whole endpoint, so
that combination is rejected at config load. Offset partitioning is the exception: it
drives the pagination parameter deliberately, and bounds each window.

The configured [rate limit](rate-limiting.md) is shared between partitions. A partitioned
table can't also have a batch [checkpoint](../configuration/checkpoints.md): it's one
position for the whole table, so a config with both is rejected at load.

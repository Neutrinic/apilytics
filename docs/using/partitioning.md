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

## Enum

When a query parameter splits the data naturally:

```hocon
partition { type = "enum", param = "kind", values = ["fire", "water", "grass"] }
```

## Date range

Splits a time window into chunks, driven by pushed-down date filters:

```hocon
partition { type = "date-range", column = "created_at", range = "7 days"
            start-param = "since", end-param = "until" }
```

## Parameters pagination already uses

Enum and date-range partitioning can't use a parameter that pagination already controls.
The paginator would overwrite it, and every partition would read the whole endpoint, so
that combination is rejected at config load. Offset partitioning is the exception: it
drives the pagination parameter deliberately, and bounds each window.

The configured [rate limit](rate-limiting.md) is shared between partitions.

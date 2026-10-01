# Configuration

Each catalog is configured by one [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md)
file: where the OpenAPI spec is, how to authenticate and paginate, and which endpoints
become tables.

```hocon
openapi = "https://api.example.com/openapi.json"

auth {
  type = "bearer"
  token = ${API_TOKEN}
}

pagination {
  style = "link_header"       # link_header | cursor | offset | none
  page-size-param = "per_page"
  max-page-size = 100
}

http {
  timeout = "30s"
  max-retries = 3
  max-backoff = "30s"
  response-format = "json"    # json | ndjson | sse
}

schema {
  flatten-depth = 2           # 0 = no flattening; deeper objects become JSON strings
  array-handling = "both"     # keep_array | explode_view | both
  mode = "strict"             # strict | variant
}

tables {
  issues {
    endpoint = "/repos/owner/repo/issues"
    filters = [
      { param = "state", column = "state", operators = ["eq"] }
    ]
  }
}
```

`openapi` can be a URL, or a file path. A relative path is resolved against the
config file's own directory.

The example configs in the repository are complete and commented, and CI loads every one
of them:

- [`examples/github/github-config.conf`](https://github.com/Neutrinic/apilytics/blob/main/examples/github/github-config.conf): every option, documented.
- [`examples/pokeapi/pokeapi-config.conf`](https://github.com/Neutrinic/apilytics/blob/main/examples/pokeapi/pokeapi-config.conf): a public API with no authentication, including a parent-child join.
- [`examples/slack/slack-config.conf`](https://github.com/Neutrinic/apilytics/blob/main/examples/slack/slack-config.conf): bearer authentication and cursor pagination.
- [`examples/lichess/lichess-ndjson.conf`](https://github.com/Neutrinic/apilytics/blob/main/examples/lichess/lichess-ndjson.conf) and [`examples/sse/sse-demo.conf`](https://github.com/Neutrinic/apilytics/blob/main/examples/sse/sse-demo.conf): streaming response formats.

## Which endpoints become tables

APIlytics only reads: it sends GET requests, and never writes to the API.

- **Discovered from the spec:** every GET operation whose `200` (or `default`) response is
  JSON and is an object, or an array of objects. Endpoints with a path placeholder, such
  as `/pokemon/{id}`, aren't listed, because nothing would supply the value; they're
  reachable as the child of a [parent-child join](../using/joins.md).
- **Named** by the operation's `operationId`, or, without one, by the path's last segment:
  `/api/v2/pokemon` becomes `pokemon`.
- **Configured tables add to the discovered ones.** Each key under `tables` is a table of
  its own, whose `endpoint` is matched against the spec, placeholders included, to find
  its schema. `SHOW TABLES` lists both.
- **Exploded tables:** with `array-handling = explode_view` or `both`, each array field
  also gets a `<table>_<field>` table. See [Schema](schema.md).

## Filters

A table's `filters` map SQL predicates to query parameters, so the API does the filtering.
Predicates without a matching filter are applied by Spark after the rows arrive, which
means reading every page. [Troubleshooting](../troubleshooting.md) shows how to see which
filters were pushed down.

## More

- [Configuration reference](../reference/configuration.md): every key and its default.
- [Credentials](credentials.md): keeping tokens out of config files.
- [Schema](schema.md): strict and variant modes, and flattening.
- [Response formats](response-formats.md): NDJSON and Server-Sent Events.
- [Checkpoints](checkpoints.md): incremental batch reads.

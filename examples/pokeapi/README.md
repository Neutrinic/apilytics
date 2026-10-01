# PokeAPI Example

Query the [PokeAPI](https://pokeapi.co) as Spark SQL tables — no authentication required.

## Quick Start

Run these from the repository root:

```bash
# Build the jar
sbt assembly

# Start the Spark cluster
docker compose -f docker/spark/compose.spark.yaml up -d

# Launch spark-shell with the PokeAPI config
./scripts/spark-shell.sh pokeapi         # Linux/macOS
.\scripts\spark-shell.ps1 pokeapi        # Windows
```

The helper script builds the jar if it's missing, but doesn't start the cluster, so start
that first. Or skip the build entirely with the Docker image:

```bash
docker run -it --rm ghcr.io/neutrinic/apilytics:latest "SHOW TABLES IN api.default"
```

## Tables

`SHOW TABLES IN api.default` lists ten:

| Table | Where it comes from |
|---|---|
| `pokemon`, `types`, `abilities` | Configured in `pokeapi-config.conf`, with `data-path = "/results"`, so each row is one record: `name` and `url`. |
| `type_pokemon` | Configured: a parent-child join, one call per row of `types`. See the comments in the config. |
| `listPokemon`, `listTypes`, `listAbilities` | Discovered from the spec's operation IDs. Each row is a page envelope: `count`, `next`, `previous` and `results`. |
| `listPokemon_results`, `listTypes_results`, `listAbilities_results` | Exploded views of `results`, one row per element, from `array-handling = both`. Columns are prefixed: `results_name`, `results_url`. |

The spec's detail endpoint, `/api/v2/pokemon/{id}`, isn't listed: endpoints with a path
placeholder are only reachable as the child of a parent-child join.

## Example Queries

```scala
// Discover all tables
spark.sql("SHOW TABLES IN api.default").show(false)

// How many Pokemon exist?
spark.sql("SELECT count, next FROM api.default.listPokemon LIMIT 1").show(false)

// List the first 10 Pokemon
spark.sql("SELECT results_name, results_url FROM api.default.listPokemon_results LIMIT 10").show(false)

// Count all Pokemon (paginates through all pages automatically)
spark.sql("SELECT count(*) AS total FROM api.default.listPokemon_results").show(false)

// Find a specific Pokemon
spark.sql("""
  SELECT results_name, results_url
  FROM api.default.listPokemon_results
  WHERE results_name = 'pikachu'
""").show(false)

// List all Pokemon types
spark.sql("SELECT results_name FROM api.default.listTypes_results LIMIT 10").show(false)

// Column pruning — only fetches the columns you select
spark.sql("SELECT results_name FROM api.default.listPokemon_results LIMIT 5").show(false)
```

## Configuration

`pokeapi-config.conf` configures offset pagination with `results-path` pointing to the array field in each response:

```hocon
pagination {
  style = offset
  offset-param = "offset"
  page-size-param = "limit"
  max-page-size = 100
  results-path = "/results"   # JSON Pointer — used for empty-page detection
}

schema {
  array-handling = both        # base tables + exploded views
}
```

Key settings:
- **`results-path`** — RFC 6901 JSON Pointer to the results array. Used by the paginator to detect empty pages and stop.
- **`array-handling = both`** — generates base tables (array serialized as JSON string) and exploded views (one row per array element) side by side.
- **`max-page-size = 100`** — PokeAPI caps pages at 100 items.

## What This Demonstrates

- **Offset pagination** with automatic termination (empty results detection)
- **Exploded array views** — array fields in API responses become queryable rows
- **Column pruning** — only requested columns are materialized
- **Zero-copy Arrow columnar reads** — API JSON responses convert directly to Arrow batches
- **No authentication** — PokeAPI is fully public, making it ideal for testing

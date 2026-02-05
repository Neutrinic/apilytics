# PokeAPI Example

Query the [PokeAPI](https://pokeapi.co) as Spark SQL tables — no authentication required.

## Quick Start

```bash
# Build the fat JAR
sbt assembly

# Start the Spark cluster
cd docker/spark
docker compose -f compose.spark.yaml up -d

# Launch spark-shell with PokeAPI config
.\scripts\spark-shell.ps1 pokeapi        # Windows
./scripts/spark-shell.sh pokeapi         # Linux/macOS
```

## Tables

The OpenAPI spec (`pokeapi-spec.yaml`) defines 3 list endpoints and 1 detail endpoint. With `array-handling = both`, this produces both base tables and exploded views for array fields:

| Table | Description |
|---|---|
| `listPokemon` | Base table — count, next, previous, results (as JSON string) |
| `listPokemon_results` | Exploded view — one row per Pokemon with name and url |
| `listTypes` | Base table — all Pokemon types |
| `listTypes_results` | Exploded view — one row per type |
| `listAbilities` | Base table — all abilities |
| `listAbilities_results` | Exploded view — one row per ability |
| `getPokemon` | Detail endpoint — stats, moves, abilities, types, sprites |
| `getPokemon_stats` | Exploded view — one row per stat |
| `getPokemon_moves` | Exploded view — one row per move |
| `getPokemon_abilities` | Exploded view — one row per ability |
| `getPokemon_types` | Exploded view — one row per type |

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

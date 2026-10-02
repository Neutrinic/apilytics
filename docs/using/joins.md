# Parent-child joins

A child table makes one extra API call per row of a parent table. The bundled PokeAPI
config has a working example. For each row of `types`, it calls that type's detail
endpoint and returns the Pokemon of that type:

```hocon
--8<-- "pokeapi/pokeapi-config.conf:parent-child"
```

```sql
SELECT _parent_type_name AS type, pokemon_name, slot
FROM api.default.type_pokemon LIMIT 5;
```

| Key | Meaning |
|---|---|
| `endpoint` | The child endpoint. `{type_name}` is filled in from the parent row. |
| `parent-table` | The table whose rows drive the calls. |
| `parent-key` | The parent column substituted into the path. |
| `data-path` | Where the child records sit in the response, when they are nested. |
| `join-strategy` | `nested_loop` makes one call per parent row. `batch` sends many parent keys in one call; see below. |

The parent key comes back as a column named `_parent_` followed by the path parameter's
name: `{type_name}` gives `_parent_type_name`.

## Batch joins

When the API can look up many parents in one call, such as `/orders?customer_ids=1,2,3`,
a batch join cuts the number of calls:

```hocon
tables {
  customers { endpoint = "/customers" }
  orders {
    endpoint        = "/orders"          # no placeholder: the keys go in batch-param
    parent-table    = "customers"
    parent-key      = "id"
    join-strategy   = "batch"
    batch-param     = "customer_ids"
    batch-size      = 100                # parent keys per call
    child-key-field = "customer_id"      # the field in each order that names its customer
  }
}
```

```sql
SELECT _parent_id AS customer, order_id FROM api.default.orders;
```

- The keys are joined by `batch-separator`, `,` by default, into `batch-param`.
- Each child record is matched back to its parent through `child-key-field`. Without it,
  common names derived from `parent-key` are tried. A child that matches no parent in its
  batch is dropped.
- With no placeholder to name it after, the parent key column is named after `parent-key`:
  `parent-key = "id"` gives `_parent_id`.

## Two things that catch people out

Missing either of these gives **zero rows and no error**:

- **`data-path` is needed when the child nests its records.** Without it, the table
  advertises the wrapper's columns while the reader reads the nested records.
- **Pagination is per endpoint.** A detail endpoint usually doesn't paginate like the list
  endpoint the source-level config was written for, so it needs
  `pagination { style = none }` or its own settings. Any table can override pagination
  this way, not only join children.

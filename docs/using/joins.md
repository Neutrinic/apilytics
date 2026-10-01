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
| `join-strategy` | `nested_loop` makes one call per parent row. |

`join-strategy = batch`, which would send many parent keys in one call for APIs that accept
a list, doesn't work yet: the config loads, but the table fails when it's built
([#277](https://github.com/Neutrinic/apilytics/issues/277)). Use `nested_loop`.

The parent key comes back as a column named `_parent_` followed by the path parameter's
name: `{type_name}` gives `_parent_type_name`.

## Two things that catch people out

Missing either of these gives **zero rows and no error**:

- **`data-path` is needed when the child nests its records.** Without it, the table
  advertises the wrapper's columns while the reader reads the nested records.
- **Pagination is per endpoint.** A detail endpoint usually doesn't paginate like the list
  endpoint the source-level config was written for, so it needs
  `pagination { style = none }` or its own settings. Any table can override pagination
  this way, not only join children.

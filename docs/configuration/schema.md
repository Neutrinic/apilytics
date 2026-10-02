# Schema

The `schema` block decides how API responses become columns.

| `mode` | Result |
|---|---|
| `strict` | The default. Typed columns from the OpenAPI schema. Nested objects are flattened to `flatten-depth`; deeper ones become JSON strings. |
| `variant` | The whole record as one native Spark `VARIANT` column. |

## Strict

Strict mode uses the OpenAPI schema to produce typed columns. Nested objects are flattened
to `flatten-depth` levels. Anything deeper becomes a JSON string, which `parse_json()`
can read if you need it.

`array-handling` controls array fields:

| `array-handling` | Result |
|---|---|
| `keep_array` | The default. Arrays stay as array columns. |
| `explode_view` | Each array field also becomes a table, `<table>_<field>`, with one row per element. `SHOW TABLES` lists only these exploded tables. |
| `both` | `SHOW TABLES` lists the base tables and the exploded ones. |

## Variant

```hocon
schema {
  mode = "variant"
}
```

```sql
-- One column, `value`, of type VARIANT
SELECT value FROM api.default.users LIMIT 5;

-- variant_get() reads typed fields, with no parse_json needed
SELECT
  variant_get(value, '$.name', 'STRING') AS name,
  variant_get(value, '$.email', 'STRING') AS email
FROM api.default.users;

-- Nested paths work too
SELECT
  variant_get(value, '$.address.city', 'STRING') AS city,
  variant_get(value, '$.stats[0].value', 'INT') AS first_stat
FROM api.default.users;
```

!!! note
    The colon syntax (`value:name::string`) in Databricks' documentation is
    Databricks-specific. Open-source Spark uses `variant_get()`.

[`examples/pokeapi/pokeapi-variant.conf`](https://github.com/Neutrinic/apilytics/blob/main/examples/pokeapi/pokeapi-variant.conf)
is a working variant-mode config.

Parent-child tables aren't supported in variant mode: loading one fails, saying so. The
source's other tables still read. Use strict mode for a source with joins.

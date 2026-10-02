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

Every column is nullable, including fields the spec marks `required`. A required field
can still be null in the data: inside an optional parent object that's missing, when it's
also declared `nullable: true`, or when the API breaks its own spec. A non-nullable column
would let Spark drop `IS NULL` filters on it, and with them those rows.

### Column types

| OpenAPI | Spark |
|---|---|
| `integer` with `format: int32` | `int` |
| `integer` with any other format, or none | `bigint` |
| `number` | `double` |
| `boolean` | `boolean` |
| `string` with `format: date` | `date` |
| `string` with `format: date-time` | `timestamp` |
| any other `string` | `string` |
| an array, or an object deeper than `flatten-depth` | `string`, holding the JSON |

An `integer` without a format has no size limit in OpenAPI, and IDs and epoch milliseconds
routinely pass the 32-bit range, so only an explicit `int32` is an `int`. A `bigint` holds
any value up to 2^63 − 1.

### Values that don't match their column

APIs don't always send what their spec says. A value with exactly one reading in its
column's type is converted:

| Column | Converted | Example |
|---|---|---|
| `int`, `bigint` | a string holding an integer | `"42"` → `42` |
| `double` | a string holding a number | `"3.5"` → `3.5` |
| `boolean` | `"true"` or `"false"`, in any case | `"TRUE"` → `true` |
| `timestamp` | a space in place of `T` | `"2024-01-02 03:04:05Z"` |
| `timestamp` | no zone, read as UTC | `"2024-01-02T03:04:05"` |

Anything else that doesn't fit or doesn't parse becomes NULL, rather than failing the read.
That includes values that would need a guess:

- a fraction in an integer column, as a number or a string: `4.2`, `"3.0"`. A JSON number
  with nothing after the point, such as `3.0`, is read as `3`.
- an `int32` value out of range. The column's type is fixed when the query is planned.
- an epoch number in a timestamp column. Seconds and milliseconds would both give a
  plausible date.
- an empty string, or any other text, that isn't a date or timestamp.

The first such NULL in each column, in each task, is logged as a warning, naming the column
and the value. Every one is counted: the scan's node in the **SQL** tab of the Spark UI
shows **values replaced with NULL**, and **values converted** beside it, so you can see what
was touched. A JSON `null` or a missing field is NULL without being counted.

`array-handling` controls array fields:

| `array-handling` | Result |
|---|---|
| `keep_array` | The default. Each array is one column holding the array as a JSON string. Read elements with `from_json` or `parse_json()`, or use `explode_view` for a row per element. |
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

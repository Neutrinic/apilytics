# JDBC and BI tools

BI tools such as DBeaver, Tableau and Power BI connect over JDBC through Spark's Thrift
server, with the APIlytics catalogs registered in the server's Spark config.

## With Docker

```bash
docker run -p 10000:10000 --rm ghcr.io/neutrinic/apilytics:latest thrift
```

The `pokeapi` and `github` catalogs are configured:

```sql
SELECT name FROM pokeapi.default.pokemon LIMIT 10;
```

## Connection details

- **Driver**: Apache Hive JDBC
- **URL**: `jdbc:hive2://localhost:10000`
- **Username**: any, e.g. `spark`
- **Password**: empty

In DBeaver: **New Connection → Apache Hive**, host `localhost`, port `10000`, then
**Test Connection**.

## On your own Spark

Start the Thrift server with the jar and a catalog per API:

```bash
--8<-- "thrift.sh"
```

Spark Connect works the same way: register the catalog in the Connect server's Spark
config, and clients query `api.default.<table>` as usual.

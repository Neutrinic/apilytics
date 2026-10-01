# Troubleshooting

## Which filters reach the API

APIlytics logs its pushdown decisions at INFO level. To see them in `spark-shell`:

```scala
import org.apache.log4j.{Logger, Level}
Logger.getLogger("com.apilytics").setLevel(Level.INFO)
Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
```

A filtered query then logs which filters were sent to the API:

```
INFO FilterPushdown: Filters pushed to API: state = 'open'
```

and which ones Spark applied itself, because the table has no matching `filters` entry:

```
INFO FilterPushdown: Filters applied locally by Spark: author = 'octocat'
```

A slow query is usually a local filter: Spark has to read every page to apply it. Add a
`filters` entry for that column if the API supports the parameter. See
[Configuration](configuration/index.md#filters).

## Shorter error messages

`sqlClean` and `withCleanErrors` replace Spark's long stack traces with the message that
matters:

```scala
import com.apilytics.spark.implicits._

spark.sqlClean("SELECT * FROM api.default.issues").show()

withCleanErrors {
  spark.sql("SELECT * FROM api.default.issues").show()
}
```

## Common problems

**A table returns zero rows, with no error.** For a [parent-child](using/joins.md) table,
check `data-path` and its pagination. Both are covered on that page.

**A catalog is missing from `SHOW CATALOGS`.** Catalogs load lazily. Query it once, for
example `SHOW NAMESPACES IN api`. See [Catalogs](using/catalogs.md).

**`<catalog>.<table>` is not found.** Tables live in the `default` namespace:
`<catalog>.default.<table>`.

**A streaming query's first run writes nothing.** That's expected with
`Trigger.AvailableNow`, which Declarative Pipelines uses. See
[Streaming](using/streaming.md#what-a-stream-delivers).

**A request fails with `No response from <host> within <timeout>`.** The host couldn't be
reached, or sent no response headers within `http.timeout`. Check that the driver and
executors have outbound access to the API: platforms that run in a private network often
need a NAT gateway or a proxy.

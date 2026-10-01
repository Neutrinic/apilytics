# Unsupported and untested

APIlytics needs Spark 4.0 or later, and a way to load a third-party jar.

| Platform | Status | Why |
|---|---|---|
| Azure HDInsight | Unsupported | Its newest Spark is 3.3. |
| Cloudera CDP | Unsupported | Cloudera Runtime 7.3.2 ships Spark 3.5.4. A stock Apache Spark 4 build on CDP's YARN is the [YARN](yarn.md) setup. |
| Snowflake | Unsupported | It doesn't run Spark, and its Spark-compatible APIs have no way to load third-party data sources. |
| Microsoft Fabric | Not run | No eligible account was available to test it. |
| Databricks on GCP | Not run | It uses the same runtime builds as AWS and Azure, which are verified. See [Databricks](databricks.md). |
| Databricks serverless | Unsupported | Serverless compute doesn't accept custom jars. |

If you run APIlytics on one of these, or on a platform not listed, an
[issue](https://github.com/Neutrinic/apilytics/issues) with the result is welcome.

$SPARK_HOME/sbin/start-thriftserver.sh \
  --packages io.github.neutrinic:apilytics_2.13:1.0.0 \
  --conf spark.sql.catalog.api=com.apilytics.spark.RESTCatalog \
  --conf spark.sql.catalog.api.config=/path/to/config.conf

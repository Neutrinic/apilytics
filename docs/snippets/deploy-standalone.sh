spark-submit --master spark://master-host:7077 \
  --packages io.github.neutrinic:apilytics_2.13:1.0.0 \
  --conf spark.sql.catalog.api=com.apilytics.spark.RESTCatalog \
  --conf spark.sql.catalog.api.config=/etc/apilytics/api.conf \
  your-job.py

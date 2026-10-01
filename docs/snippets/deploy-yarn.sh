spark-submit --master yarn --deploy-mode cluster \
  --packages io.github.neutrinic:apilytics_2.13:1.0.0 \
  --files api.conf,api-spec.yaml \
  --conf spark.sql.catalog.api=com.apilytics.spark.RESTCatalog \
  --conf spark.sql.catalog.api.config=api.conf \
  your-job.py

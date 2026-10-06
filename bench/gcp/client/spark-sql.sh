#!/bin/bash
# Runs a SQL file against the apilytics catalog `api` in local mode.
# usage: spark-sql.sh <cores> <apilytics-conf> <sql-file>
exec /opt/spark/bin/spark-sql --master "local[$1]" --driver-memory 8g --jars /opt/bench/apilytics.jar \
  --conf spark.sql.catalogImplementation=in-memory --conf spark.ui.enabled=false \
  --conf spark.sql.catalog.api=com.apilytics.spark.RESTCatalog --conf spark.sql.catalog.api.config="$2" -f "$3"

#!/usr/bin/env python3
"""
Basic PySpark example for Apilytics.

Usage (inside Docker container):
    python3 /opt/apilytics/examples/pyspark/basic.py

Or with spark-submit:
    spark-submit /opt/apilytics/examples/pyspark/basic.py
"""

from pyspark.sql import SparkSession

# Create SparkSession with Apilytics catalog configured
spark = SparkSession.builder \
    .appName("Apilytics PySpark Example") \
    .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog") \
    .config("spark.sql.catalog.api.config", "/opt/apilytics/examples/pokeapi/pokeapi-config.conf") \
    .getOrCreate()

# Query Pokemon API
print("\n=== Pokemon (first 5) ===")
df = spark.sql("SELECT name, url FROM api.default.pokemon LIMIT 5")
df.show(truncate=False)

# Show schema
print("\n=== Schema ===")
df.printSchema()

# DataFrame API works too
print("\n=== Using DataFrame API ===")
pokemon = spark.table("api.default.pokemon")
pokemon.select("name").show(5)

spark.stop()

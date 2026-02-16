#!/usr/bin/env python3
"""
PySpark example querying GitHub Issues API.

Requires GITHUB_TOKEN environment variable.

Usage:
    GITHUB_TOKEN=ghp_xxx docker run -e GITHUB_TOKEN -it neutrinic/apilytics \
        python3 /opt/apilytics/examples/pyspark/github_issues.py
"""

from pyspark.sql import SparkSession
import os

# Check for token
if not os.environ.get("GITHUB_TOKEN"):
    print("Warning: GITHUB_TOKEN not set. Some queries may fail.")

spark = SparkSession.builder \
    .appName("GitHub Issues Example") \
    .config("spark.sql.catalog.api", "com.apilytics.spark.RESTCatalog") \
    .config("spark.sql.catalog.api.config", "/opt/apilytics/configs/github-public.conf") \
    .getOrCreate()

# Query issues
print("\n=== Recent Issues ===")
df = spark.sql("""
    SELECT number, title, state, created_at
    FROM api.default.issues
    LIMIT 10
""")
df.show(truncate=40)

# Aggregation example
print("\n=== Issues by State ===")
spark.sql("""
    SELECT state, COUNT(*) as count
    FROM api.default.issues
    GROUP BY state
""").show()

spark.stop()

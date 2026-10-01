# your-job.py: the catalog `api` is registered by the submit command's --conf flags.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("apilytics-job").getOrCreate()

spark.sql("SHOW TABLES IN api.default").show()
spark.sql("SELECT * FROM api.default.issues LIMIT 10").show()

spark.stop()

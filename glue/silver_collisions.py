'''
Initializes Spark session, prints a tests message, and shuts down cleanly.

'''

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("NYC Collision Silver ETL job started successfully")

spark.stop()
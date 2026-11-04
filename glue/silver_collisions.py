'''


'''

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name,
    col,
    to_date,
    hour,
    dayofweek,
    month,
    to_timestamp,
    concat_ws,
    count,
    sum as spark_sum,
    when,
)
# Read arguments passed from the Glue job configuration
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BRONZE_BUCKET"])
# Retrieves the bucket name we configured in CDK
bronze_bucket = args["BRONZE_BUCKET"]

# Entry point for processing data with Spark.
spark = SparkSession.builder.getOrCreate()

# Read Bronze collision JSON files
bronze_path = f"s3://{bronze_bucket}/collisions/"

df = (
    spark.read
    # Allows Spark to parse JSON records formatted across multiple lines.
    .option("multiLine", "true")
    # Includes files in nested directories, including rolling-lookback folders.
    .option("recursiveFileLookup", "true")
    # Loads JOSN records from S3 into a Spark DataFrame
    .json(bronze_path)
)

# Track the source file for each record
df = df.withColumn("source_file", input_file_name())

# Inspect the data
print("=== BRONZE DATA PROFILE ===")
print(f"Total records: {df.count()}")
print(f"Total columns: {len(df.columns)}")

df.printSchema()

# Showing first 5 rows of which columns, of dataframe.
df.select(
    "collision_id",
    "crash_date",
    "crash_time",
    "latitude",
    "longitude",
    "source_file",
).show(5, truncate=False)


# Convert raw string columns into analytical data types
silver_df = (
    df
    .withColumn("crash_date", to_date(col("crash_date")))
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn(
        "crash_timestamp",
        to_timestamp(
            concat_ws(" ", col("crash_date"), col("crash_time")),
            "yyyy-MM-dd HH:mm",
        ),
    )
    .withColumn("crash_hour", hour(col("crash_timestamp")))
    .withColumn("crash_day_of_week", dayofweek(col("crash_date")))
    .withColumn("crash_month", month(col("crash_date")))
)

print("=== SILVER TRANSFORMATION PREVIEW ===")

silver_df.select(
    "collision_id",
    "crash_date",
    "crash_timestamp",
    "crash_hour",
    "crash_day_of_week",
    "latitude",
    "longitude",
).show(10, truncate=False)

silver_df.printSchema()

print("=== SILVER VALIDATION ===")

df.select(
    count("*").alias("total_rows"),
    count("crash_date").alias("raw_crash_date"),
    count("crash_time").alias("raw_crash_time"),
    count("latitude").alias("raw_latitude"),
    count("longitude").alias("raw_longitude"),
).show(truncate=False)

silver_df.select(
    count("crash_date").alias("crash_date"),
    count("latitude").alias("latitude"),
    count("longitude").alias("longitude"),
    count("crash_timestamp").alias("crash_timestamp"),
    count("crash_hour").alias("crash_hour"),
    count("crash_day_of_week").alias("crash_day_of_week"),
    count("crash_month").alias("crash_month"),
).show(truncate=False)


print("=== CONVERSION FAILURES ===")

silver_df.select(
    spark_sum(
        when(
            col("crash_time").isNotNull()
            & col("crash_timestamp").isNull(),
            1,
        ).otherwise(0)
    ).alias("timestamp_failures"),

    spark_sum(
        when(
            col("crash_date").isNotNull()
            & col("crash_month").isNull(),
            1,
        ).otherwise(0)
    ).alias("month_failures"),
).show(truncate=False)

spark.stop()
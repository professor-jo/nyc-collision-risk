'''
Silver ETL for NYC collision data.

Reads raw Bronze JSON, converts columns to analytical data types, derives
temporal columns, and prints validation, data quality and duplicate results.
'''

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    count,
    dayofweek,
    hour,
    input_file_name,
    lit,
    month,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
    when,
    regexp_extract,
    row_number,
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
    # Loads JSON records from S3 into a Spark DataFrame
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


# All injury and fatality columns
count_columns = [
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_motorist_injured",
    "number_of_motorist_killed",
]

# Convert numeric strings to integers
# We are ensuring numerical columns have numerical data types
for column_name in count_columns:
    silver_df = silver_df.withColumn(
        column_name,
        col(column_name).cast("int")
    )

# Check collision ID
silver_df = silver_df.withColumn(
    "invalid_id",
    col("collision_id").isNull()
    | (trim(col("collision_id")) == "")
)

# Check crash date
silver_df = silver_df.withColumn(
    "invalid_date",
    col("crash_date").isNull()
)

# Check negative injury/fatality counts
negative_condition = lit(False)

for column_name in count_columns:
    # A null count is not negative. Without coalesce, the condition would be
    # null and the row would be dropped by both filters below.
    negative_condition = (
        negative_condition
        | coalesce(col(column_name) < 0, lit(False))
    )

silver_df = silver_df.withColumn(
    "invalid_counts",
    negative_condition
)

# Check coordinates against an approximate NYC bounding box
silver_df = silver_df.withColumn(
    "invalid_coordinates",
    (
        col("latitude").isNotNull()
        & col("longitude").isNotNull()
        & ~(
            col("latitude").between(40.4, 41.0)
            & col("longitude").between(-74.3, -73.6)
        )
    )
)

# Critical errors = records cannot be safely entered in Silver.
critical_error = (
    col("invalid_id")
    | col("invalid_date")
    | col("invalid_counts")
)

# A collision can still be useful for temporal and
# severity analysis without usable coordinates.

valid_df = silver_df.filter(~critical_error)
quarantine_df = silver_df.filter(critical_error)


valid_count = valid_df.count()
unique_id_count = valid_df.select("collision_id").distinct().count()

print("=== P2.5 DATA QUALITY RESULTS ===")

print("Total records:", silver_df.count())
print("Valid records:", valid_count)
print("Quarantined records:", quarantine_df.count())

silver_df.groupBy(
    "invalid_id",
    "invalid_date",
    "invalid_counts",
    "invalid_coordinates",
).count().show(truncate=False)

print("=== DUPLICATE ANALYSIS ===")

print("Unique collision IDs:", unique_id_count)
print("Duplicate occurrences:", valid_count - unique_id_count)

spark.stop()

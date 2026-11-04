'''
Silver ETL for NYC collision data.

Reads raw Bronze JSON, converts columns to analytical data types, derives
temporal columns, applies data quality checks, and keeps the latest record
for each collision.
'''

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    dayofweek,
    hour,
    input_file_name,
    lit,
    month,
    regexp_extract,
    row_number,
    sha2,
    struct,
    to_date,
    to_json,
    to_timestamp,
    trim,
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

# Extracting ingestion date from the S3 file path.
valid_df = valid_df.withColumn(
    "ingestion_date",
    # Converts extracted text into a Spark date.
    to_date(
        # regexp_extract() searches the S3 path for ingestion date.
        regexp_extract(
            col("source_file"),
            r"ingestion_date=(\d{4}-\d{2}-\d{2})",
            1,
        ),
        "yyyy-MM-dd",
    )
)

print("=== INGESTION DATE COVERAGE ===")

valid_df.groupBy(
    col("ingestion_date").isNull().alias("missing_ingestion_date")
).count().show()

# Create a reproducible hash from each record's content.
# This helps break ties between different versions
# stored in the same source file.
hash_columns = sorted(valid_df.columns)

valid_df = valid_df.withColumn(
    "record_hash",
    sha2(
        to_json(
            struct(*[col(name) for name in hash_columns])
        ),
        256,
    ),
)

# Define the ordering within each collision ID.
window_spec = (
    Window
    .partitionBy("collision_id")
    .orderBy(
        col("ingestion_date").desc_nulls_last(),
        col("source_file").desc(),
        col("record_hash").desc(),
    )
)

# Assign a rank to every version of each collision.
ranked_df = valid_df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

# Keep only the first record for each collision.
deduplicated_df = (
    ranked_df
    .filter(col("row_num") == 1)
    .drop("row_num", "record_hash")
)

after_count = deduplicated_df.count()

remaining_duplicates = (
    after_count
    - deduplicated_df.select("collision_id").distinct().count()
)

print("=== P2.6 DEDUPLICATION RESULTS ===")

print("Records before:", valid_count)
print("Records after:", after_count)
print("Records removed:", valid_count - after_count)
print("Duplicate IDs remaining:", remaining_duplicates)

print("=== INGESTION DATE DISTRIBUTION ===")

deduplicated_df.groupBy(
    "ingestion_date"
).count().orderBy(
    col("ingestion_date").desc_nulls_last()
).show(20, truncate=False)

# Fail the job if deduplication does not meet expectations.
assert after_count == unique_id_count, (
    "Deduplicated count does not match unique collision count"
)

assert remaining_duplicates == 0, (
    "Duplicate collision IDs remain"
)

spark.stop()

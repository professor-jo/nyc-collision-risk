'''
Silver ETL for NYC collision data.

Reads raw Bronze JSON, converts columns to analytical data types, derives
temporal columns, applies data quality checks, and keeps the latest record
for each collision.
'''

import sys
import boto3

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    dayofweek,
    hour,
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
    greatest,
    udf,
)

from pyspark.sql.types import StringType

import h3

def coordinates_to_h3(latitude, longitude, resolution):
    if latitude is None or longitude is None:
        return None
    if not (40.4 <= latitude <= 41.0):
        return None
    if not (-74.3 <= longitude <= -73.6):
        return None

    return h3.latlng_to_cell(
        latitude,
        longitude,
        resolution,
    )
h3_res8_udf = udf(
    lambda lat, lon: coordinates_to_h3(lat,lon,8),
    StringType(),
)

h3_res9_udf = udf(
    lambda lat, lon: coordinates_to_h3(lat,lon,9),
    StringType()
)

# Read arguments passed from the Glue job configuration
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BRONZE_BUCKET", "SILVER_BUCKET"]
)

# Retrieves the bucket name we configured in CDK
bronze_bucket = args["BRONZE_BUCKET"]
silver_bucket = args["SILVER_BUCKET"]

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

# Track the source file for each record.
# _metadata.file_path is deterministic, unlike input_file_name(), which
# MERGE INTO rejects in its source plan.
df = df.select("*", col("_metadata.file_path").alias("source_file"))

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

# Read modification timestamps for Bronze S3 files.
s3_client = boto3.client("s3")

paginator = s3_client.get_paginator("list_objects_v2")

file_metadata = []

for page in paginator.paginate(
    Bucket=bronze_bucket,
    Prefix="collisions/",
):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".json"):
            file_metadata.append(
                (
                    f"s3://{bronze_bucket}/{obj['Key']}",
                    obj["LastModified"],
                )
            )

metadata_df = spark.createDataFrame(
    file_metadata,
    ["source_file", "source_modified_at"],
)

# Attach S3 timestamps to the corresponding collision records.
valid_df = valid_df.join(
    metadata_df,
    on="source_file",
    how="left",
)

missing_timestamps = valid_df.filter(
    col("source_modified_at").isNull()
).count()

print("Missing S3 modification timestamps:", missing_timestamps)

# Show both sets of paths so a failed join can be diagnosed from the job log.
if missing_timestamps > 0:
    print("=== UNMATCHED SOURCE FILES ===")
    valid_df.filter(
        col("source_modified_at").isNull()
    ).select("source_file").distinct().show(20, truncate=False)

    print("=== S3 METADATA PATHS ===")
    metadata_df.select("source_file").show(20, truncate=False)

assert missing_timestamps == 0, (
    "Some Bronze records have no S3 modification timestamp"
)

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

valid_df = valid_df.withColumn(
    "source_version_at",
    greatest(
        col("ingestion_date").cast("timestamp"),
        col("source_modified_at"),
    ),
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
        col("source_version_at").desc_nulls_last(),
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

# Columns used only for ETL validation.
temporary_columns = [
    "invalid_id",
    "invalid_date",
    "invalid_counts",
    "location",
]

# Keep a clean, flat schema for analytical queries.
# invalid_coordinates stays so spatial queries can exclude out-of-bounds rows.
published_df = deduplicated_df.drop(*temporary_columns)

published_df = (
    published_df
    .withColumn(
        "h3_res8",
        h3_res8_udf(col("latitude"), col("longitude")),    
    )
    .withColumn(
        "h3_res9",
        h3_res9_udf(col("latitude"), col("longitude")),
    )
)

print("=== FINAL SILVER SCHEMA ===")
published_df.printSchema()

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

table_name = "glue_catalog.nyc_collision_lakehouse.silver_collisions"

table_location = f"s3://{silver_bucket}/tables/silver_collisions/"

published_df.createOrReplaceTempView("silver_updates")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    USING iceberg
    PARTITIONED BY (months(crash_date))
    LOCATION '{table_location}'
    TBLPROPERTIES (
        'format-version' = '2',
        'write.format.default' = 'parquet'
    )
    AS SELECT * FROM silver_updates WHERE 1 = 0
""")

# Add H3 columns to the existing Iceberg table.
existing_columns = {
    field.name
    for field in spark.table(table_name).schema.fields
}

for column_name in ["h3_res8", "h3_res9"]:
    if column_name not in existing_columns:
        spark.sql(
            f"ALTER TABLE {table_name} "
            f"ADD COLUMNS ({column_name} STRING)"
        )

# Insert new collisions and update existing ones in a single operation.
# A row is replaced only when the incoming version is newer, so re-running
# the job on older Bronze files never overwrites fresher data.
spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING silver_updates AS source
    ON target.collision_id = source.collision_id

    WHEN MATCHED AND (
        source.source_version_at > target.source_version_at

        OR (
            source.source_version_at = target.source_version_at
            AND source.source_file > target.source_file
        )

        OR (
            source.source_version_at = target.source_version_at
            AND source.source_file = target.source_file
            AND (
                (target.h3_res8 IS NULL AND source.h3_res8 IS NOT NULL)
                OR
                (target.h3_res9 IS NULL AND source.h3_res9 IS NOT NULL)
            )
        )
    )
    THEN UPDATE SET *

    WHEN NOT MATCHED
    THEN INSERT *
""")

# Validate the persisted table, not just the in-memory DataFrame.
result = spark.sql(f"""
    SELECT
        COUNT(*) AS total_records,
        COUNT(DISTINCT collision_id) AS unique_records
    FROM {table_name}
""").first()

print("=== P2.7 ICEBERG VALIDATION ===")
print("Total records:", result["total_records"])
print("Unique collision IDs:", result["unique_records"])

assert result["total_records"] == result["unique_records"], (
    "Duplicate collision IDs found in Silver Iceberg table"
)

spark.stop()

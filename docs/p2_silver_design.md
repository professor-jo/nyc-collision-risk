
# P2 Silver Lakehouse Design

## Storage
- Bronze: Existing S3 bucket, unchanged.
- Silver: Dedicated S3 bucket managed by AWS CDK.
- Table format: Apache Iceberg.
- Data format: Parquet with Snappy compression.

## Glue Data Catalog
- Database: nyc_collision_lakehouse
- Table: silver_collisions

## Data Model
- Logical primary key: collision_id (STRING).
- Crash date: DATE.
- Coordinates: DOUBLE, nullable.
- Injury and fatality counts: INTEGER.
- Geographic enrichment: H3 cell identifier.
- Source metadata: ingestion date, object modification time, source file.

## Partitioning
- Partition by month(crash_date).
- Let Iceberg manage physical partitions.

## Deduplication
- One Silver row per collision_id.
- Prefer the most recently ingested record.
- Resolve ties deterministically using source metadata.
- Preserve support for legacy lookback paths.

## Invalid Records
- Quarantine records with invalid IDs, dates, or severity counts.
- Retain records with missing geographic coordinates.
- Do not silently discard invalid records.

## Processing
- AWS Glue PySpark.
- Incremental Iceberg MERGE INTO.
- Idempotent reruns.
- Athena for SQL validation.

## Cost Controls
- Run Glue on demand during development.
- Use minimal practical worker capacity.
- Avoid unnecessary full-table rewrites.
- Monitor S3, Glue, and Athena costs.
```

### What happens after this?

Once you've saved this file, **P2.1 will be complete** and we'll start P2.2 — AWS Glue Infrastructure.

Our first implementation task will be defining the Glue IAM role using AWS CDK, where you'll learn how Glue obtains permission to read Bronze data and write Silver data.

No deployment is needed yet.
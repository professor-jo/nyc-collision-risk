# Project Progress

| Phase | Goal | Status |
|-------|------|--------|
| P0: Cloud Foundations | Set up a secure, reproducible AWS environment | ✅ Complete |
| P1: Batch Data Ingestion | Collect NYC collision data automatically and store it safely | ✅ Complete |
| P2: Silver Layer | Turn raw, duplicated JSON into a clean, queryable Iceberg table | ✅ Core implementation complete |
| P3: Pipeline Orchestration | Run ingestion and Silver as one scheduled, safe pipeline | 🟡 Design complete |

`infra/infra/infra_stack.py` is one CDK stack shared by all three phases.

---

## P0 — Cloud Foundations Completion Checklist

**Status: ✅ Complete**

### Checklist

#### Infrastructure as Code

- [x] Set up an AWS CDK app in Python.
- [x] Define all infrastructure in a single CDK stack.
- [x] Pass environment-specific settings (budget email) as CDK context.
- [x] Pin CDK and test dependencies.

#### Storage

- [x] Create Bronze, Silver and Gold S3 buckets.
- [x] Enable S3-managed encryption.
- [x] Require HTTPS for all bucket access.
- [x] Block all public access.
- [x] Enable versioning and expire old versions after 30 days.
- [x] Retain buckets if the stack is deleted.

#### Access & Cost Control

- [x] Use IAM Identity Center (SSO) instead of IAM users or long-lived access keys.
- [x] Grant service permissions in the stack as each service is added.
- [x] Create an AWS Budgets email alert when monthly spend passes $5.
- [x] Tag resources for the project.

#### Testing

- [x] Unit-test security, retention, tagging and budget settings before deployment.

---

### Final Outcome

**P0 — Cloud Foundations: COMPLETE ✅**

Built a secure, reproducible AWS foundation defined entirely in code, with encrypted, private, versioned storage for each lakehouse layer and a spending alert.

**Key Technologies:** AWS CDK, Amazon S3, IAM Identity Center, AWS Budgets.

**Key Files:** `infra/app.py`, `infra/cdk.json`, `infra/infra/infra_stack.py`, `infra/tests/unit/test_infra_stack.py`.

**Next Phase:** P1 — Batch Data Ingestion.

---

## P1 — Batch Data Ingestion Completion Checklist

**Status: ✅ Complete**

### Checklist

#### Ingestion

- [x] Fetch collision records from the NYC Open Data API.
- [x] Ingest in time-budgeted batches with keyset pagination.
- [x] Validate records before upload.
- [x] Store raw JSON in S3 Bronze, organized by ingestion date.

#### Reliability

- [x] Track progress with a checkpoint stored in S3.
- [x] Advance the checkpoint only after an upload succeeds.
- [x] Prevent overlapping runs with a DynamoDB lock that expires.
- [x] Re-fetch the previous 14 days to capture late-arriving records.
- [x] Keep the lookback from moving the checkpoint, and upload only complete windows.

#### Automation & Monitoring

- [x] Run ingestion in AWS Lambda.
- [x] Schedule ingestion daily at 00:00 UTC with EventBridge.
- [x] Send email alerts for Lambda errors through CloudWatch and SNS.
- [x] Alert when the lookback stays incomplete three days in a row.

#### Build & Testing

- [x] Build the Lambda package reproducibly with pinned dependencies.
- [x] Unit-test checkpointing, recovery, batching, the lock, the handler and the lookback.
- [x] Document operation, deployment and limitations in the README.

---

### Final Outcome

**P1 — Batch Data Ingestion: COMPLETE ✅**

Built an automated, incremental, serverless pipeline that collects NYC collision data every day and preserves it unchanged in S3 Bronze, recovering safely from failures and late-arriving records.

**Key Technologies:** AWS Lambda, Amazon EventBridge, Amazon DynamoDB, Amazon S3, Amazon CloudWatch, Amazon SNS, NYC Open Data API.

**Key Files:** `ingestion/lambda_handler.py`, `ingestion/ingest_collisions.py`, `ingestion/checkpoint.py`, `ingestion/lock.py`, `ingestion/lookback_collisions.py`, `ingestion/config.py`, `scripts/build_lambda_package.sh`, `tests/`.

**Next Phase:** P2 — Silver Layer.

---

## P2 — Silver Layer Completion Checklist

**Status: ✅ Core Implementation Complete**

### Checklist

#### Architecture & Infrastructure

- [x] Profile Bronze schema and duplicates.
- [x] Design Bronze-to-Silver ETL architecture.
- [x] Define Silver schema and transformation rules.
- [x] Provision AWS Glue infrastructure using CDK.
- [x] Configure IAM permissions for Bronze and Silver S3 buckets.
- [x] Create Glue Data Catalog database.
- [x] Deploy Glue PySpark ETL job.
- [x] Configure Apache Iceberg integration.

#### Data Extraction & Transformation

- [x] Read Bronze JSON files recursively from S3.
- [x] Track source files and S3 modification timestamps.
- [x] Convert dates, coordinates, and injury counts into appropriate data types.
- [x] Generate temporal features (hour, day of week, month).
- [x] Implement data quality validation.
- [x] Flag invalid coordinates and critical errors.
- [x] Deduplicate records using `collision_id`.
- [x] Implement deterministic source-version ordering.

#### Iceberg & Incremental Processing

- [x] Create Silver Apache Iceberg table.
- [x] Store processed data as Parquet in S3.
- [x] Partition records by crash month.
- [x] Implement incremental `MERGE INTO`.
- [x] Insert new records and update newer collision versions.
- [x] Validate persisted record uniqueness.
- [x] Verify idempotency through repeated Glue executions.

#### H3 Geographic Enrichment

- [x] Implement H3 coordinate conversion.
- [x] Generate `h3_res8` and `h3_res9` identifiers.
- [x] Handle missing and invalid coordinates.
- [x] Configure H3 dependency in Glue.
- [x] Evolve Iceberg schema and backfill existing records.
- [x] Validate H3 geographic coverage and consistency.

#### Athena & Integration Testing

- [x] Configure Athena query-result storage.
- [x] Query Silver Iceberg table using SQL.
- [x] Verify record counts and uniqueness.
- [x] Validate missing and invalid fields.
- [x] Execute H3 geographic hotspot aggregation.
- [x] Verify Iceberg schema and monthly partitioning.
- [x] Confirm Parquet storage and ZSTD compression.
- [x] Successfully execute end-to-end Glue ETL pipeline.

#### Follow-ups

- [ ] Persist quarantined records. They are counted but not saved anywhere.
- [ ] Define the Athena query-result location in CDK. It is currently set only in the console.
- [ ] Publish an explicit Silver column list, so new Bronze fields can't break `MERGE INTO`.
- [ ] Investigate the ~2,000 Bronze records stored outside an `ingestion_date=` folder.
- [ ] Update the README's "What Silver must do" section to the `source_version_at` ordering.

---

### Final Outcome

**P2 — Silver Layer: COMPLETE ✅**

Built and deployed an AWS Glue PySpark ETL pipeline that transforms raw NYC collision data from Amazon S3 Bronze into a validated, deduplicated, and geographically enriched Apache Iceberg Silver table.

**Latest Run:** 31,296 Bronze records → 8,275 unique collisions, 0 duplicates. All 8,030 collisions with usable coordinates have H3 cells. The remaining 245 have missing or out-of-bounds coordinates.

**Key Technologies:** AWS Glue, PySpark, Amazon S3, Apache Iceberg, AWS Glue Data Catalog, Amazon Athena, H3, AWS CDK.

**Key Files:** `glue/silver_collisions.py`, `docs/p2_silver_design.md`, `infra/infra/infra_stack.py`, `scripts/inspect_bronze_schema.py`, `scripts/inspect_bronze_duplicates.py`.

**Next Phase:** P3 — Pipeline Orchestration with AWS Step Functions.

---

## P3 — Pipeline Orchestration Checklist

**Status: 🟡 Design Complete, Build Not Started**

### Checklist

#### Design

- [x] Inspect existing P1 Lambda/EventBridge triggers and P2 Glue inputs/outputs.
- [x] Define pipeline run ID, date window, watermark and replay/backfill parameters.
- [x] Design state transitions and success/failure contracts.
- [x] Decide how to prevent overlapping runs and duplicate processing.

#### Build

- [ ] Return a structured result from the ingestion Lambda and raise `IngestionLockHeld` instead of returning 409.
- [ ] Run the lookback only on the final catch-up loop.
- [ ] Add a backfill mode that fetches a date range without moving the watermark.
- [ ] Set Glue `MaxConcurrentRuns = 1` and accept `--PIPELINE_RUN_ID`.
- [ ] Build the Step Functions state machine with the pipeline lock, catch-up loop and Silver gate.
- [ ] Point the daily EventBridge rule at the state machine instead of the Lambda.
- [ ] Add SNS failure notification and an `ExecutionsFailed` alarm.
- [ ] Test scheduled, replay, backfill and lock-held runs end to end.

**Key Files:** `docs/p3_orchestration_design.md`.

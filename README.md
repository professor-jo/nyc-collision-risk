# nyc-collision-risk

An end-to-end data project on NYC motor vehicle collision data, built on AWS.

- **Data engineering:** ingest raw collision data and move it through a bronze → silver → gold data lake on AWS
- **Data analysis:** SQL analysis of geographic hotspots, time patterns, risk factors and severity
- **Data science / ML:** forecast which areas are most likely to see collisions

All infrastructure is defined as code with the **AWS CDK (Python)** in [`infra/`](infra/).

## Tech stack

AWS CDK (Python) · Amazon S3 · AWS Budgets · pytest

## Project phases

| Phase | Scope | Status |
|-------|-------|--------|
| P0 | Infrastructure foundation | In progress |

## Deploying

`lambda_package/` is generated and not committed. The CDK CLI rebuilds it with `scripts/build_lambda_package.sh` before every synth, diff and deploy:

```bash
cd infra && cdk deploy -c budget_email=you@example.com
```

Run the script yourself before running the infra tests on a fresh clone, since they need `lambda_package/` to exist.

Ingestion stops with an error if its checkpoint is missing, so a new environment needs one created once, after the first deploy. This command refuses to overwrite an existing checkpoint:

```bash
BRONZE_BUCKET=<bucket-name> python -m ingestion.checkpoint
```

## Late-arriving records

NYC can publish a collision after the watermark has passed its crash date. So after each incremental run, the Lambda re-fetches the `LOOKBACK_DAYS` (14, in [`ingestion/config.py`](ingestion/config.py)) before the watermark, plus the watermark's own date, and stores them under `collisions/lookback/`.

The lookback never moves the watermark, and it only uploads complete windows. If it runs short of time it uploads nothing and logs `LOOKBACK_STATUS=incomplete`, and the next run replays the whole window. An alarm fires if that happens three days in a row. Its window overlaps earlier ingestion, so bronze holds duplicates on purpose: Silver must deduplicate by `collision_id` and keep the row from the latest `ingestion_date`.

---

## Progress log

### P0: Infrastructure foundation

I've implemented the following:

- Set up a CDK Python project to define all infrastructure as code
- Created three S3 buckets (bronze, silver, gold) for a medallion data lake
- Blocked all public access to the buckets to prevent accidental data exposure
- Encrypted data at rest with S3-managed keys and required HTTPS for encryption in transit
- Enabled versioning to recover from accidental overwrites or deletes
- Added lifecycle rules that expire old versions after 30 days and clean up failed uploads after 7 days, to control storage cost
- Set buckets to be retained if the stack is deleted, so data is never lost by accident
- Tagged every bucket with project and layer for cost tracking
- Added a $5/month AWS Budget with an email alert as a cost guardrail, with the email passed in at deploy time so it's never committed
- Wrote 11 CDK assertion unit tests to verify the security, retention, tagging and budget settings before anything is deployed

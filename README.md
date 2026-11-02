# nyc-collision-risk

An end-to-end data project on NYC motor vehicle collision data, built on AWS.

- **Data engineering:** ingest raw collision data and move it through a bronze → silver → gold data lake on AWS
- **Data analysis:** SQL analysis of geographic hotspots, time patterns, risk factors and severity
- **Data science / ML:** forecast which areas are most likely to see collisions

All infrastructure is defined as code with the **AWS CDK (Python)** in [`infra/`](infra/).

## Tech stack

Python 3.12 · AWS Lambda · Amazon EventBridge · Amazon S3 · Amazon DynamoDB · Amazon CloudWatch · Amazon SNS · AWS Budgets · AWS CDK (Python) · pytest

## Project phases

| Phase | Scope | Status |
|-------|-------|--------|
| P0 | Infrastructure foundation | Complete |
| P1 | Incremental ingestion into Bronze | Deployed and scheduled; first scheduled run still to be verified |
| Later | Silver and Gold layers, analysis, ML | Not started |

## Architecture

```text
EventBridge (daily, 00:00 UTC)
        |
        v
Ingestion Lambda ---- acquire / release ----> DynamoDB lock table
        |
        |  fetch pages  <-------------------- NYC Open Data API (h9gi-nx95)
        |
        |  write ---------------------------> S3 Bronze
        |                                       collisions/ingestion_date=.../
        |                                       collisions/lookback/ingestion_date=.../
        |                                       state/ingestion_watermark.json
        v
CloudWatch alarms ---> SNS email alert
```

One run does the following:

1. **Take the lock.** If another run holds it, return `statusCode: 409` and skip. The lock expires after 10 minutes, so a crashed run can't block ingestion for good.
2. **Ingest new records.** Read the checkpoint, fetch 100 records at a time with keyset pagination on `(crash_date, collision_id)`, validate each batch, upload it, and only then advance the checkpoint. This stops after 240 seconds, or when under 60 seconds of Lambda time remain, and the next run resumes from the checkpoint.
3. **Re-fetch recent dates.** See [Late-arriving records](#late-arriving-records).
4. **Release the lock**, whether or not the run succeeded.

The Silver and Gold buckets exist, but nothing writes to them yet.

## Testing

```bash
python -m venv .venv && source .venv/bin/activate
pip install boto3 -r requirements-lambda.txt -r infra/requirements.txt -r infra/requirements-dev.txt

# The infra tests need lambda_package/ to exist, and it isn't committed
bash scripts/build_lambda_package.sh

# Ingestion modules read this on import. The tests mock AWS and the API.
export BRONZE_BUCKET=test-bronze-bucket
python -m pytest tests/ infra/tests/ -q
```

## Deploying

`lambda_package/` is generated and not committed. The CDK CLI rebuilds it with `scripts/build_lambda_package.sh` before every synth, diff and deploy:

```bash
cd infra
cdk diff -c budget_email=you@example.com
cdk deploy -c budget_email=you@example.com
```

Ingestion stops with an error if its checkpoint is missing, so a new environment needs one created once, after the first deploy. This command refuses to overwrite an existing checkpoint:

```bash
BRONZE_BUCKET=<bucket-name> python -m ingestion.checkpoint
```

Then confirm the SNS subscription email, because alarm emails aren't delivered until you do.

## Running it manually

A run can take a few minutes, but the AWS CLI's default read timeout is 60 seconds, and it retries when that expires. The retry starts a second invocation, which gets a misleading 409 from the lock. Set a read timeout longer than the Lambda's 5-minute limit:

```bash
aws lambda invoke \
  --function-name <function-name> \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  --cli-connect-timeout 30 \
  --cli-read-timeout 360 \
  response.json
```

## Late-arriving records

NYC can publish a collision after the watermark has passed its crash date. So after each incremental run, the Lambda re-fetches the `LOOKBACK_DAYS` (14, in [`ingestion/config.py`](ingestion/config.py)) before the watermark, plus the watermark's own date, and stores them under `collisions/lookback/`.

The lookback never moves the watermark, and it only uploads complete windows. If it runs short of time it uploads nothing and logs `LOOKBACK_STATUS=incomplete`, and the next run replays the whole window.

### What Silver must do

The lookback window overlaps earlier ingestion, so Bronze holds duplicates on purpose. Silver must keep one row per `collision_id`, chosen by this order:

1. The latest `ingestion_date`, read from the object's path.
2. If those match, the newest S3 `LastModified`.
3. If those match too, the lexicographically largest object key.

Bronze has no extraction timestamp yet, and two different snapshots from one day share an `ingestion_date`, which is why steps 2 and 3 are needed. `LastModified` only has one-second resolution, and copying or restoring an object changes it. A later improvement is to store an extraction timestamp in object metadata or a manifest.

Silver also has to handle two oddities in existing data:

- **The November 1 lookback file** sits at `collisions/lookback/start_date=2026-10-01/...`, with no `ingestion_date` in its path, so use its `LastModified` date instead. Keep this file: it's the only copy of collision `f6c26572e89b284a9ddc83fdb1007a8f`, which the incremental batches missed.
- **Ten rows in `ingestion_date=2026-10-30`** have crash dates from 2013 to 2025, before the pipeline's October 1, 2026 starting point.

### How the lookback alarm behaves

`LookbackIncompleteAlarm` fires when each of three consecutive UTC days has at least one `LOOKBACK_STATUS=incomplete` log line. It counts days, not scheduled runs:

- A day with no incomplete line counts as healthy, even if the lookback never reported. So the alarm stays quiet if the schedule stops, and incomplete days separated by a day with no status line never add up.
- If the incremental step fails, or Lambda times out mid-lookback, no status line is logged. `IngestionErrorAlarm` covers those failures, but nothing alerts on a run that never started.
- Manual invokes count too, and a complete run doesn't cancel an incomplete one on the same day.

A more robust later option is to emit an explicit completion metric for every scheduled run and alarm when it's missing.

## Operational limitations

- **Missed runs aren't detected.** No alarm fires if the schedule stops or a run never starts.
- **Skipped runs don't alarm.** A 409 counts as a successful Lambda invocation.
- **One slow request can outlast the time checks.** The time checks run before each request, but a single request with retries can take several minutes. If Lambda times out, the data stays safe and the error alarm fires. The lock stays held for up to 10 minutes, so Lambda's automatic retries may get a 409, and the next day's run catches up.
- **Very late records can still be missed.** A record published when its crash date is already more than 14 days behind the watermark falls outside the window.
- **The lookback window can't grow much without other changes.** A 14-day window took about 2 minutes of the 5-minute limit. A 30-day window would probably need a longer Lambda timeout and lock duration.
- **Bronze grows by about 3 MB per day**, because each run with changed data stores a new copy of the window.

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

### P1: Incremental ingestion

I've implemented the following:

- Built a Lambda that loads new collisions from the NYC Open Data API into Bronze, with an S3 checkpoint it only advances after an upload succeeds
- Used keyset pagination on `(crash_date, collision_id)`, with validation of each batch for duplicates, order and position after the checkpoint
- Made uploads idempotent, so re-running after a failure skips identical batches and refuses to overwrite different data
- Added a DynamoDB lock with an expiry to prevent overlapping runs
- Added a time budget, so long backlogs finish over several runs
- Added a rolling 14-day lookback for late-arriving records, kept separate from the checkpoint
- Scheduled a daily run with EventBridge, with CloudWatch alarms for Lambda errors and persistently incomplete lookbacks, sent by email through SNS
- Made the Lambda package build reproducible, with pinned dependencies
- Wrote unit tests for checkpointing, failure recovery, batching, the lock, the lookback and the alarm

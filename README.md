# nyc-collision-risk

An end-to-end data project on NYC motor vehicle collision data, built on AWS.

- **Data engineering:** ingest raw collision data and move it through a bronze → silver → gold data lake on AWS
- **Data analysis:** SQL analysis of geographic hotspots, time patterns, risk factors and severity
- **Data science / ML:** forecast which areas are most likely to see collisions

All infrastructure is defined as code with the **AWS CDK (Python)** in [`infra/`](infra/).

## Tech stack

Python 3.12 · AWS Step Functions · AWS Lambda · AWS Glue (PySpark) · Apache Iceberg · Amazon Athena · H3 · Amazon EventBridge · Amazon S3 · Amazon DynamoDB · Amazon SQS · Amazon CloudWatch · Amazon SNS · AWS Budgets · AWS CDK (Python) · pytest

## Project phases

| Phase | Scope | Status |
|-------|-------|--------|
| P0 | Infrastructure foundation | Complete |
| P1 | Incremental ingestion into Bronze | Complete |
| P2 | Silver Iceberg table with H3 enrichment | Complete |
| P3 | Step Functions orchestration | Deployed; first scheduled execution still to be verified |
| Later | Gold layer, analysis, ML | Not started |

## Architecture

```text
EventBridge (daily, 00:00 UTC) ---- undelivered events ----> SQS dead-letter queue
        |  {"trigger": "scheduled"}
        v
Step Functions: nyc-collision-pipeline
        |  acquire / release ---------------> DynamoDB lock table
        |
        |  up to 5 attempts, until caught up
        v
Ingestion Lambda
        |  fetch pages  <-------------------- NYC Open Data API (h9gi-nx95)
        |
        |  write ---------------------------> S3 Bronze
        |                                       collisions/ingestion_date=.../
        |                                       collisions/lookback/ingestion_date=.../
        |                                       collisions/backfill/ingestion_date=.../
        |                                       state/ingestion_watermark.json
        v
Glue PySpark job (only when Bronze changed, or on replay)
        |  MERGE INTO ----------------------> S3 Silver (Apache Iceberg)
        |                                       nyc_collision_lakehouse.silver_collisions
        v
CloudWatch alarms ---> SNS email alert
```

One run does the following:

1. **Check the input.** A backfill without both dates is rejected before the lock is taken.
2. **Take the pipeline lock.** If another execution holds it, a run with `"trigger": "scheduled"` ends as a successful skip, and any other run fails with `PipelineLockUnavailable`. The lock expires after 50 minutes, the 45-minute workflow timeout plus 5 minutes for a Lambda still running, so a stopped execution can't block the pipeline for good.
3. **Ingest new records.** Each Lambda attempt reads the checkpoint, fetches 100 records at a time with keyset pagination on `(crash_date, collision_id)`, validates each batch, uploads it, and only then advances the checkpoint. An attempt stops after 240 seconds, or when under 60 seconds of Lambda time remain. Step Functions runs up to 5 attempts until ingestion has caught up. The Lambda also holds its own 10-minute lock, which protects direct invocations.
4. **Re-fetch recent dates** once ingestion has caught up, or on the last attempt. See [Late-arriving records](#late-arriving-records).
5. **Run Silver** if any attempt changed Bronze, including when ingestion didn't catch up, so that data still reaches Silver. See [Silver quality checks](#silver-quality-checks).
6. **Release the pipeline lock.**
7. **Report the outcome.** A run that caught up succeeds, and one that didn't catch up within 5 attempts fails with `IngestionIncomplete`. A Lambda or Glue error releases the lock and fails with the original error through `PipelineFailed`.

The Gold bucket exists, but nothing writes to it yet.

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

## AWS test results

These ran against the deployed stack on November 8–9, 2026.

| Test | Execution or method | Result |
|---|---|---|
| Normal run, no new data | `manual-e2e-20261108-145746` | Succeeded in 171 s. Glue was correctly skipped, and the lock was released |
| Replay with the real Glue job | `manual-replay-20261108-150058` | Succeeded in 246 s, skipped ingestion, and the lock was released |
| Silver quality checks in that replay | Glue job log | 34,913 Bronze records, 0 quarantined. The table had 8,275 records, 8,275 unique, and all six quality rules returned 0 |
| Controlled failure (backfill with reversed dates) | `manual-failure-test-20261108-150556` | Failed with the real `ValueError` through `PipelineFailed`, and the lock was released |
| Alert delivery | That failure | `PipelineExecutionsFailedAlarm` and `IngestionErrorAlarm` both sent to SNS, and an alert email was received |
| Lock expiry and takeover | `test-state` and a throwaway DynamoDB item | An active lock blocked a second writer, and an expired lock could be taken over |
| Input and mode routing | `test-state` | Missing fields, replay, backfill and normal runs all routed correctly |
| EventBridge cutover | Live rule and Lambda policy | The rule's only target is the state machine with `{"trigger": "scheduled"}`, and EventBridge can no longer invoke the Lambda |

**Not yet exercised on AWS:** a scheduled execution, new Bronze data triggering Glue automatically, a successful backfill, a real lock conflict, a stopped-execution recovery, and a real Glue failure. These paths are covered by unit tests that walk the generated state machine.

## Deploying

`lambda_package/` is generated and not committed. The CDK CLI rebuilds it with `scripts/build_lambda_package.sh` before every synth, diff and deploy:

```bash
cd infra
cdk diff -c budget_email=you@example.com -c dbt_client_cidr=$(curl -s https://checkip.amazonaws.com)/32
cdk deploy -c budget_email=you@example.com -c dbt_client_cidr=$(curl -s https://checkip.amazonaws.com)/32
```

Ingestion stops with an error if its checkpoint is missing, so a new environment needs one created once, after the first deploy. This command refuses to overwrite an existing checkpoint:

```bash
BRONZE_BUCKET=<bucket-name> python -m ingestion.checkpoint
```

Then confirm the SNS subscription email, because alarm emails aren't delivered until you do.

## Running it manually

Start the workflow with one of these inputs:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:nyc-collision-pipeline \
  --input '{"trigger": "manual"}'
```

| Input | What it does |
|---|---|
| `{"trigger": "manual"}` | Normal ingestion, then Silver if Bronze changed |
| `{"trigger": "manual", "mode": "replay"}` | Skips ingestion and always runs Silver. Use it after fixing the Silver code or a failed Silver run |
| `{"trigger": "manual", "mode": "backfill", "backfill_start": "2026-10-01", "backfill_end": "2026-10-15"}` | Re-fetches that range into `collisions/backfill/` without moving the watermark. `backfill_end` is excluded, and the range must fit in one Lambda run, or it fails with `BackfillIncomplete` |

If an execution is stopped, the pipeline lock clears by itself after 50 minutes. To rerun sooner, first confirm no execution is running, then delete the item with `lock_id = nyc_collision_pipeline` from the lock table.

To invoke the Lambda directly, remember that a run can take a few minutes, but the AWS CLI's default read timeout is 60 seconds, and it retries when that expires. The retry starts a second invocation, which fails with a misleading `IngestionLockHeld` error. Set a read timeout longer than the Lambda's 5-minute limit:

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

NYC can publish a collision after the watermark has passed its crash date. So once ingestion has caught up, or on the last attempt, the Lambda re-fetches the `LOOKBACK_DAYS` (14, in [`ingestion/config.py`](ingestion/config.py)) before the watermark, plus the watermark's own date, and stores them under `collisions/lookback/`.

The lookback never moves the watermark, and it only uploads complete windows. If it runs short of time or raises an error, it uploads nothing and logs `LOOKBACK_STATUS=incomplete`, and the next run replays the whole window. A lookback error doesn't fail the run, so the Lambda still reports what ingestion wrote.

### How Silver picks the latest record

The lookback window overlaps earlier ingestion, so Bronze holds duplicates on purpose. Silver keeps one row per `collision_id`, chosen by this order:

1. The newest `source_version_at`, which is the later of the object path's `ingestion_date` and its S3 `LastModified`.
2. If those match, the lexicographically largest source file.
3. If those match too, the largest record hash, so the result is repeatable.

Bronze has no extraction timestamp yet, which is why the order relies on these fields. `LastModified` only has one-second resolution, and copying or restoring an object changes it. A later improvement is to store an extraction timestamp in object metadata or a manifest.

Two oddities in the existing data:

- **The November 1 lookback file** sits at `collisions/lookback/start_date=2026-10-01/...`, with no `ingestion_date` in its path, so its `LastModified` sets its version. Keep this file: it's the only copy of collision `f6c26572e89b284a9ddc83fdb1007a8f`, which the incremental batches missed.
- **Ten rows in `ingestion_date=2026-10-30`** have crash dates from 2013 to 2025, before the pipeline's October 1, 2026 starting point.

### How the lookback alarm behaves

`LookbackIncompleteAlarm` fires when each of three consecutive UTC days has at least one `LOOKBACK_STATUS=incomplete` log line. It counts days, not scheduled runs:

- A day with no incomplete line counts as healthy, even if the lookback never reported. So the alarm stays quiet if the schedule stops, and incomplete days separated by a day with no status line never add up.
- If the incremental step fails, or Lambda times out mid-lookback, no status line is logged. `IngestionErrorAlarm` covers those failures, but nothing alerts on a run that never started.
- Manual invokes count too, and a complete run doesn't cancel an incomplete one on the same day.

A more robust later option is to emit an explicit completion metric for every scheduled run and alarm when it's missing.

## Silver quality checks

The Glue job fails, and so does the pipeline, if a check doesn't pass.

- **Before writing:** there must be valid records, and no more than 1% of rows can be quarantined. A sudden jump usually means the source format changed.
- **After `MERGE INTO`:** the saved table must have unique IDs, every collision the run published, no missing IDs, crash dates or `source_version_at`, no negative counts, and H3 cells on exactly the rows with usable coordinates.

The after-write checks run once the data is committed, so a failure there alerts you but doesn't undo the write.

## Alerts

Each of these emails the SNS alert topic:

| Alarm | Fires when |
|---|---|
| `PipelineExecutionsFailedAlarm` | An execution fails, including a manual lock conflict or a run that didn't catch up |
| `PipelineExecutionsTimedOutAlarm` | An execution passes the 45-minute timeout |
| `PipelineScheduleDeliveryAlarm` | EventBridge couldn't start the workflow and the event reached the dead-letter queue |
| `IngestionErrorAlarm` | The Lambda raises an error |
| `LookbackIncompleteAlarm` | The lookback is incomplete three days in a row |

Stopped executions and skipped scheduled runs don't alert.

## Operational limitations

- **A disabled schedule isn't detected.** The dead-letter queue catches events EventBridge couldn't deliver, but no alarm fires if the rule is disabled or never triggers.
- **Overlapping Lambda runs alarm.** A held Lambda lock raises `IngestionLockHeld`, which counts as a Lambda error and triggers `IngestionErrorAlarm`. Inside the pipeline, the pipeline lock normally prevents this.
- **A failed attempt skips Silver for that run.** If one attempt writes Bronze and a later attempt's Lambda fails, Glue doesn't run. The next run that changes Bronze picks that data up, because Silver reads all of Bronze, or you can start a replay.
- **A large backlog fails visibly.** A run that doesn't catch up in 5 attempts still updates Silver, then fails with `IngestionIncomplete`. The next day continues from the watermark.
- **Silver cost grows with Bronze.** Each run reads every Bronze file.
- **A failing lookback only alarms after three days.** Lookback errors are logged as incomplete rather than raised, so `IngestionErrorAlarm` doesn't fire and Lambda doesn't retry. The next run replays the window.
- **One slow request can outlast the time checks.** The time checks run before each request, but a single request with retries can take several minutes. If Lambda times out, the data stays safe and the error alarm fires. The lock stays held for up to 10 minutes, so Lambda's automatic retries may fail with `IngestionLockHeld`, and the next day's run catches up.
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

### P2: Silver layer

I've implemented the following:

- Built a Glue PySpark job that turns raw Bronze JSON into typed, validated records with temporal features
- Deduplicated records by `collision_id` with a deterministic version order
- Published to an Apache Iceberg table partitioned by crash month, using an idempotent `MERGE INTO`
- Added H3 resolution 8 and 9 cells for geographic analysis
- Verified the table in Athena: 31,296 Bronze records became 8,275 unique collisions at the time

### P3: Pipeline orchestration

I've implemented the following:

- Built a Step Functions workflow with a catch-up loop of up to 5 Lambda attempts and a conditional Glue step
- Added a DynamoDB pipeline lock that expires, so stopped or timed-out executions recover by themselves
- Separated outcomes: successful runs, scheduled skips, runs that didn't catch up, and failures that keep the original error
- Added replay and backfill modes that never move the watermark
- Added Silver quality gates before and after the write
- Added CloudWatch alarms and an SQS dead-letter queue, with alerts by email through SNS
- Moved the daily EventBridge schedule from the Lambda to the state machine
- Tested the workflow on AWS, with the results in [AWS test results](#aws-test-results)

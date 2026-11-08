# P3 Pipeline Orchestration Design

Status: proposal. Sections 1 and 2 describe what exists today; sections 3 to 6 are recommendations to agree on before building.

## 1. Current P1 ingestion

### Trigger
- EventBridge rule `DailyCollisionIngestion` invokes the Lambda directly at 00:00 UTC.
- EventBridge invokes Lambda asynchronously, so Lambda retries a failed run up to two more times by default.
- Nothing runs Silver afterwards.

### Lambda (`ingestion/lambda_handler.py`)
- Timeout 5 minutes, memory 256 MB.
- Order: acquire lock → `main()` (incremental ingest) → `run_lookback()` → release lock in `finally`.
- Returns:
  - `{"statusCode": 200}` when ingestion and lookback finish.
  - `{"statusCode": 409}` when the lock is held. This is a normal return, not an error, so callers can't tell a skipped run from a successful one without reading the body.
  - Raises on any other failure.
- The return value doesn't say how many records were ingested, whether ingestion caught up with the API, or whether the lookback was complete.
- `main()` stops after `MAX_INGESTION_SECONDS` (240 s) even when more records remain. A backlog therefore takes several daily runs to clear.
- A lookback that runs out of time only prints `LOOKBACK_STATUS=incomplete`, which a CloudWatch metric filter counts.

### Lock (`ingestion/lock.py`)
- DynamoDB item `lock_id = nyc_collision_ingestion`, owner UUID, `expires_at` = now + 600 s.
- Acquired with a conditional put; released only by its owner.

### Watermark (`ingestion/checkpoint.py`)
- `s3://<bronze>/state/ingestion_watermark.json` with `last_crash_date` and `last_collision_id`.
- Advanced only after each batch is uploaded. The lookback never moves it.

### Bronze outputs
- Incremental: `collisions/ingestion_date=YYYY-MM-DD/collisions_<crash_date>_<collision_id>.json`
- Lookback: `collisions/lookback/ingestion_date=YYYY-MM-DD/start_date=…/end_date=…/batch_<checksum>.json`
- Legacy: about 2,000 records sit in files outside any `ingestion_date=` folder.
- Writes are idempotent: an identical object is skipped, and a conflicting one raises an error.

## 2. Current P2 Silver job

### Trigger
- Manual only (`aws glue start-job-run`).

### Glue job `nyc-collision-silver-etl`
- Glue 5.0, 2 × G.1X, 10-minute timeout, 0 retries.
- No `MaxConcurrentRuns` is set, so Glue's default of 1 applies. A second start while one is running fails with `ConcurrentRunsExceededException`.
- Arguments: `--BRONZE_BUCKET`, `--SILVER_BUCKET`.

### Inputs
- Every JSON file under `collisions/` on every run, plus an S3 listing for modification times. There is no date window, so cost and runtime grow with Bronze.

### Outputs
- `MERGE INTO glue_catalog.nyc_collision_lakehouse.silver_collisions`: inserts new collisions and updates a row only with a newer `source_version_at`.
- Reruns are idempotent.

### Success contract
- The job state is `SUCCEEDED`.
- Data problems fail the job through assertions: missing timestamps, leftover duplicates, or a non-unique table.
- Results are only printed to the log. Nothing machine-readable records what a run did.

## 3. Run parameters (proposal)

The Step Functions execution input:

```json
{
  "trigger": "scheduled",
  "mode": "scheduled | replay | backfill",
  "run_date": "2026-11-08",
  "backfill_start": "2026-10-01",
  "backfill_end": "2026-10-15",
  "run_silver": true
}
```

### Pipeline run ID
- Use the Step Functions execution name:
  - `scheduled-<run_date>` for the daily run.
  - `replay-<timestamp>` and `backfill-<start>-<end>-<timestamp>` for manual runs.
- Standard workflows reject a duplicate execution name for 90 days, so a scheduled day can't run twice by accident.
- Pass it to Glue as `--PIPELINE_RUN_ID` so logs and later run summaries can be traced back to it.

### Date window
- **Scheduled:** no window. Ingestion continues from the watermark, and the lookback covers the 14 days before it.
- **Backfill:** `backfill_start` and `backfill_end`, fetched like the lookback and written to `collisions/backfill/ingestion_date=…/…`.
- **Silver:** keeps reading all of Bronze for now, which takes about 2.5 minutes at 31k records. Once that becomes too slow, add an `--INGESTION_DATE_FROM` argument and read only newer files.

### Watermark
- Owned by the ingestion Lambda alone. Step Functions never reads or writes it.
- Backfills and replays must never move it, the same rule as the lookback.

### Replay
- Runs Silver only, with no ingestion. Safe because `MERGE` is idempotent.
- Use it after fixing the Silver code, or to retry a failed Silver step.

### Backfill
- Re-fetches a date range into Bronze, then runs Silver.
- Silver keeps the newest version of each collision by `source_version_at`, so backfilled files can't overwrite fresher data.

## 4. State machine (proposal)

```
EventBridge (00:00 UTC) ──► Step Functions: nyc-collision-pipeline
                                  │
                       AcquirePipelineLock (DynamoDB conditional put)
                                  │  lock held ──► Succeed("skipped")
                                  ▼
                 Choice: mode == replay? ──yes──────────────┐
                                  │ no                       │
                                  ▼                          │
                   Ingest (Lambda, synchronous invoke)       │
                                  │                          │
                 Choice: caught_up? ─no (≤ 5 loops)─► Ingest │
                                  │ yes                      │
                                  ▼                          │
        Choice: bronze_changed_any or run_silver? ─no─► Release ► Succeed("no new data")
                                  │ yes                      │
                                  ▼ ◄────────────────────────┘
                 RunSilver (glue:startJobRun.sync)
                                  │
                         ReleasePipelineLock
                                  │
                               Succeed

Any failure ──► Catch ──► ReleasePipelineLock ──► Notify (SNS) ──► Fail
```

### Retries

| State | Retry on | Don't retry on |
|---|---|---|
| Ingest | `Lambda.ServiceException`, `Lambda.TooManyRequestsException`, `Lambda.SdkClientException`: 2 attempts with backoff | Errors raised by the ingestion code, such as validation failures or conflicting objects |
| RunSilver | `Glue.ConcurrentRunsExceededException`: 3 attempts, 60 s backoff | Job failure. Assertions mean bad data, and retrying won't fix it |

## 5. Success and failure contracts (proposal)

### Lambda
Return a structured result, and raise instead of returning an error code:

```json
{
  "statusCode": 200,
  "caught_up": false,
  "bronze_changed": true,
  "lookback_ran": false,
  "lookback_complete": null
}
```

- A held Lambda lock raises a named error, `IngestionLockHeld`, so Step Functions can catch it.
- An incomplete lookback is still a success, as now, and so is a lookback that raises an error. The next run replays the window, and the existing three-day alarm stays.

### Glue
- Success: the job run is `SUCCEEDED`.
- Later: write `s3://<silver>/runs/<pipeline_run_id>.json` with the record, duplicate, merge and validation counts, so results don't live only in the logs.

### Pipeline
- Success: the execution ends in `Succeed`.
- Its output records whether the run was `skipped`, had `no new data`, or completed, with the Lambda and Glue results.
- Failure: the execution ends in `Fail` after an SNS email. Add a CloudWatch alarm on the state machine's `ExecutionsFailed` metric.

## 6. Preventing overlap and duplicate processing (proposal)

Use layered defenses. Each layer covers a gap in the others.

1. **Deterministic execution names.** `scheduled-<run_date>` stops the same day from running twice, even if EventBridge delivers the event twice.
2. **Pipeline lock.** A DynamoDB item `lock_id = nyc_collision_pipeline`, held by the state machine from start to finish.
   - Expires after 50 minutes: the 45-minute workflow timeout plus 5 minutes for a Lambda that's still running. A new execution can take over an expired lock, which recovers from stopped or timed-out executions.
   - It stops a manual replay or backfill from overlapping a scheduled run, which execution names alone can't do.
   - Use the existing lock table with a new `lock_id`.
3. **Keep the Lambda lock.** It still protects direct invocations outside Step Functions, such as testing with `aws lambda invoke`.
4. **Glue `MaxConcurrentRuns = 1`.** Set it explicitly in CDK as a backstop, rather than relying on the default.
5. **Idempotent writes.** These already exist. Bronze skips identical objects, and the Silver `MERGE` only replaces a row with a newer version. If a run is repeated anyway, the result is the same.

### Infrastructure changes
- Point the EventBridge rule at the state machine instead of the Lambda. Keeping both targets would run ingestion twice.
- Invoke the Lambda synchronously from Step Functions, which removes Lambda's async retries.
- Grant the state machine role:
  - `lambda:InvokeFunction`
  - `glue:StartJobRun`, `glue:GetJobRun`, `glue:BatchStopJobRun`
  - DynamoDB `PutItem` and `DeleteItem` on the lock table
  - `sns:Publish`

## Decisions

| Decision | Choice | Reason |
|---|---|---|
| 1. Run Silver when there's no new data? | Skip, except in `replay` mode | Avoid unnecessary Glue compute costs |
| 2. Catch-up strategy? | Loop up to 5 Lambda runs | Recover from backlogs without waiting several days |
| 3. Pipeline lock already held? | Scheduled: `Succeed("skipped")`. Manual: `Fail` | Avoid noisy scheduled alerts while making manual conflicts visible |

### What each decision means for the build
1. **Silver gate:** after ingestion and before the lock is released, a `Choice` state runs Silver only when `bronze_changed_any` is true, or when `mode == replay`.
   - `bronze_changed` must include lookback and backfill writes as well as incremental ones, so late-arriving records still reach Silver.
   - Silver also runs when ingestion didn't catch up, so data from those attempts isn't stranded in Bronze.
   - The `run_silver` input can force a run in any mode.
2. **Catch-up loop:** a counter in the execution state starts at 1. After each `Ingest`, loop again while `caught_up == false` and the counter is below 5.
   - Each Lambda run takes about 4 minutes, so the worst case is about 20 minutes. This fits inside the 45-minute workflow timeout, together with a 10-minute Glue run.
   - A backlog still unfinished after 5 runs releases the lock and ends in `Fail` with error `IngestionIncomplete`, so a stuck backlog is visible. The next day continues from the watermark.
   - The lookback runs once, when ingestion has caught up or on the final attempt, so it isn't repeated up to 5 times.
3. **Held lock:**
   - For `trigger == scheduled`, `AcquirePipelineLock`'s `ConditionalCheckFailedException` goes to the `ScheduledRunSkipped` success state. No email is sent.
   - For any other trigger, or none, it goes to `Fail` with error `PipelineLockUnavailable`, which triggers the failure alarm.

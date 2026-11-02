'''
This file is to retrieve late-arriving collision records from the NYC Data.


late-arriving records = records that were recorded aafter its actual date.


1. A collision happened on January 1st.
2. BUT the collision was recorded on NYC data on January 7th.
3. We fetch the collision records on January 8th.
4. This file will fetch late-arriving records recorded between Jan 1st ~ 7th
5. Therefore late-arriving records also get recorded and won't get filtered out.

The lookback only adds complete windows to S3 and never moves the watermark. The window
overlaps earlier ingestion, so Silver must deduplicate by collision_id. The README
documents which row to keep.
'''

from datetime import datetime, timedelta, timezone
from ingestion.config import BATCH_SIZE, LOOKBACK_DAYS
from ingestion.checkpoint import load_watermark
from ingestion.ingest_collisions import fetch_records, validate_records
import boto3
import hashlib
import json
import os
from botocore.exceptions import ClientError

def calculate_lookback_start(last_crash_date, days=LOOKBACK_DAYS):
    '''
    Calculate the start of a historical lookback window.

    Example:
        Last processed crash date = October 8th
        Lookback period = 14 days
        Lookback start = September 24th
    '''

    # Convert the timestamp string into a datetime object
    watermark_date = datetime.fromisoformat(last_crash_date)

    # Subtract the lookback period from the watermark date
    lookback_start = watermark_date - timedelta(days=days)

    # Convert the datetime back into an API-compatible string.
    return lookback_start.isoformat(timespec="milliseconds")




def calculate_lookback_end(last_crash_date):
    '''
    Calculate the exclusive end of the lookback window.

    The window includes the watermark's own date, because a late record
    on that date can sort below the watermark's collision_id.

    Example:
        Last processed crash date = October 8th
        Lookback end = October 9th
    '''

    watermark_date = datetime.fromisoformat(last_crash_date)
    lookback_end = watermark_date + timedelta(days=1)

    return lookback_end.isoformat(timespec="milliseconds")




def fetch_lookback_records(start_date, end_date, context=None):
    """
    Fetch all collisions within a date range, one batch at a time.

    Pages through the range with its own local position, so the
    ingestion watermark is neither read nor changed here.

    Returns the records and whether the whole range was fetched.
    """

    all_records = []

    # An empty collision_id sorts before every real ID.
    position = (start_date, "")

    while True:

        # Stop before starting another batch if time is low.
        if context is not None:
            remaining_seconds = (
                context.get_remaining_time_in_millis() / 1000
            )

            if remaining_seconds < 60:
                print(
                    "Stopping lookback early: "
                    f"only {remaining_seconds:.1f}s remaining"
                )
                return all_records, False

        records = fetch_records(position, end_date=end_date)

        if not records:
            break

        position = validate_records(records, position)
        all_records.extend(records)

        if len(records) < BATCH_SIZE:
            break

    return all_records, True



def upload_lookback_to_s3(records, start_date, end_date):
    '''
    Store a historical collision batch in S3 Bronze

    - Re-running an identical batch will skip the upload.
    - A conflicting batch will raise an error.

    '''
    if not records:
        print("No historical records to upload.")
        return

    bucket = os.environ.get("BRONZE_BUCKET")
    s3 = boto3.client("s3")

    # Serialize the records consistently
    json_data = json.dumps(
        records,
        sort_keys=True,
        separators=(",", ":")
    ).encode("utf-8")

    # Create a unique fingerprint for this batch
    checksum = hashlib.sha256(json_data).hexdigest()

    # Identify the historical window in S3
    start_day = start_date[:10]
    end_day = end_date[:10]

    # Silver uses ingestion_date to choose the latest version of a record.
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    object_key = (
        f"collisions/lookback/"
        f"ingestion_date={today}/"
        f"start_date={start_day}/"
        f"end_date={end_day}/"
        f"batch_{checksum}.json"
    )
    # Check whether the identical object already exists in S3.

    print("Bucket:", bucket)
    print("Object key:", repr(object_key))
    print("AWS region:", s3.meta.region_name)

    try:
        s3.head_object(
            Bucket=bucket,
            Key=object_key
        )

    except ClientError as error:
        error_code = error.response["Error"]["Code"]

        if error_code in ("404", "NoSuchKey", "NotFound"):
            print("S3 object does not exist. Proceeding with upload.")
        else:
            raise

    else:
        print("Identical lookback batch already exists in S3.")
        print("Skipping upload.")
        return

    # Upload only if the object was not found.
    s3.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=json_data,
        ContentType="application/json",
        Metadata={"sha256": checksum},
        IfNoneMatch="*"
    )

    print(f"Uploaded {len(records)} records to S3.")
    print(f"S3 location: s3://{bucket}/{object_key}")

def run_lookback(context=None):
    '''
    Re-fetch the window before the watermark and store it in S3 Bronze.
    '''

    last_crash_date, _ = load_watermark()

    start_date = calculate_lookback_start(last_crash_date)
    end_date = calculate_lookback_end(last_crash_date)

    records, complete = fetch_lookback_records(
        start_date,
        end_date,
        context=context,
    )

    print(f"Lookback window: {start_date} to {end_date}")
    print(f"Fetched {len(records)} lookback records")

    # Every lookback file is a complete window, so Silver never reads a
    # partial snapshot. Nothing is saved here, so the next run replays it all.
    if not complete:
        # A CloudWatch alarm in InfraStack watches for this exact text.
        print("LOOKBACK_STATUS=incomplete: nothing uploaded")
        return

    upload_lookback_to_s3(records, start_date, end_date)

    print("LOOKBACK_STATUS=complete")


############################## Running the script ##########################################

# Run with: BRONZE_BUCKET=<bucket-name> python -m ingestion.lookback_collisions
if __name__ == "__main__":
    run_lookback()

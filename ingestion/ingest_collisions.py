"""
P1: Incremental Data Ingestion.

Downloads new NYC car crash records and saves them to S3 (Bronze layer).

"Incremental" means each run only grabs crashes it hasn't seen before.

What one run does:
    1. Read the watermark: a bookmark of the last crash we saved.
    2. Ask the NYC Open Data API for crashes after that bookmark,
       100 at a time, until caught up or out of time.
    3. Check the data: no duplicates, correct order, all actually new.
    4. Upload the raw records to S3 as one JSON file.
    5. Move the bookmark forward so the next run starts from here.

How the bookmark works:
    It stores two values: (crash_date, collision_id).
    - crash_date puts crashes in time order.
    - collision_id is a random-looking ID with no meaningful order,
      so it's only used to tell apart crashes on the same date.

Usage (run from the repo root so the ingestion package resolves correctly):
    BRONZE_BUCKET=<bucket-name> python -m ingestion.ingest_collisions
"""

import json
import os
import time
from datetime import datetime, timezone
import hashlib
from botocore.exceptions import ClientError

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.config import API_URL, BATCH_SIZE, MAX_INGESTION_SECONDS
from ingestion.checkpoint import load_watermark, save_watermark

# Configuration
# Set S3 bucket for raw data, read from an environment variable
# (the script fails immediately if BRONZE_BUCKET is not set)
BUCKET_NAME = os.environ["BRONZE_BUCKET"]



def fetch_records(watermark, end_date=None):
    """Fetch the next batch of collision records after the watermark.

    Input:  watermark: tuple[str, str]
            end_date: str | None -> only return crash dates before this one
    Output: list[dict]
    """
    session = create_session()

    last_date, last_id = watermark

    where = (
        f"crash_date > '{last_date}' OR "
        f"(crash_date = '{last_date}' "
        f"AND collision_id > '{last_id}')"
    )

    if end_date is not None:
        where = f"crash_date < '{end_date}' AND ({where})"

    params = {
        "$where": where,
        "$order": "crash_date ASC, collision_id ASC",
        "$limit": BATCH_SIZE,
    }

    response = session.get(
        API_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    records = response.json()

    print(f"Fetched {len(records)} records")

    return records



def validate_records(records, watermark):
    """
    Validate uniqueness, ordering, and checkpoint of data.

    Raises an error if anything looks wrong. Otherwise returns
    the key of the last record, which becomes the new watermark.

    Input:  records: list[dict] -> raw collision records
            watermark: tuple[str, str] -> (crash_date, collision_id)
    Output: tuple[str, str] -> (crash_date, collision_id) of the newest record
    """

    # Build a (crash_date, collision_id) pair for each record
    keys = [
        (record["crash_date"], record["collision_id"])
        for record in records
    ]



    collision_ids = [key[1] for key in keys]

    # Check 1: no collision appears twice
    if len(collision_ids) != len(set(collision_ids)):
        raise ValueError("Duplicate collision IDs detected")

    # Check 2: records arrived in the order we asked for
    if keys != sorted(keys):
        raise ValueError("Records are not sorted by crash_date, collision_id")
    # Check 3: every record is newer than the old watermark
    if not all(key > watermark for key in keys):
        raise ValueError("Records contain keys below the watermark")

    print("Validation passed")

    # Records are sorted, so the last one is the newest
    return keys[-1]



def upload_to_s3(records, last_key):
    """Upload collision records to the S3 Bronze bucket.

    *Applied idempotency

    This function is IDEMPOTENT:
    - If the batch doesn't exist, upload it
    - If the identical batch exists, skip the upload
    - If the same filename exists with different data, raise an error

    This prevents accidental duplicate uploads when the pipeline
    is rerun after a failure.

    Input:  records: list[dict] -> raw collision records
            last_key: tuple[str, str] -> (crash_date, collision_id) of the newest record
    Output: None (uploads one JSON object to S3)
    """

    # Create an S3 client using our AWS credentials
    # boto3 is the Python library for interacting with AWS services
    s3 = boto3.client("s3")


    # Get today's date in UTC, eg. "2026-10-31".
    # We use this date to organize uploaded files into folders.
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Define the location and filename of the JSON object inside S3.
    object_key = (
        f"collisions/ingestion_date={today}/"
        f"collisions_{last_key[0][:10]}_{last_key[1]}.json"
    )

    # Convert the Python list of collision records into JSON bytes.
    json_data = json.dumps(
        records,
        sort_keys=True,
        separators=(",", ":")
    ).encode("utf-8")

    # Generate a fingerprint of the JSON contents
    checksum = hashlib.sha256(json_data).hexdigest()

    # Check whether this object already exists
    # Before uploading, check whether an object with the same S3 bucket
    # name and object key already exists.
    try:
        existing = s3.head_object(
            Bucket=BUCKET_NAME,
            Key=object_key
        )
    # If S3 returns an error, inspect what kind of error occurred.
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
        # Extract the AWS error code
        if error_code not in ("404", "NoSuchKey", "NotFound"):
            raise

    # The else block runs ONLY if object with this key already exists.
    else:
        existing_checksum = existing.get(
            "Metadata", {}
        ).get("sha256")

        if existing_checksum == checksum:
            print("Identical batch already exists. Skipping upload.")
            return

        raise ValueError(
            "S3 object already exists with different contents: "
            f"s3://{BUCKET_NAME}/{object_key}"
        )

    # If execution reaches this point, the object does not exist.
    # We can upload the new batch S3.
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
        Body=json_data,
        ContentType="application/json",
        Metadata={"sha256": checksum},
        IfNoneMatch="*"
    )
    # This message appears only after a successful new upload.
    print(f"Uploaded to s3://{BUCKET_NAME}/{object_key}")



# Exponential Backoff
# Increasing the delay between tries.
# It is for situations when the API fails to give data on requests.
def create_session():
    """Create an HTTP session with automatic retries.

    Failed GET requests are retried up to 3 times, waiting
    longer before each retry (exponential backoff).

    Input:  none
    Output: requests.Session -> session with retry rules attached
    """
    retry_strategy = Retry(
        # Give up after 3 retries
        total=3,
        # Wait roughly 1s, 2s, 4s between retries
        backoff_factor=1,
        # Retry on rate limiting (429) and server errors (5xx)
        status_forcelist=[429, 500, 502, 503, 504],
        # Only retry GET, which is safe to repeat
        allowed_methods=["GET"],
        # If the server says how long to wait, wait that long
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()

    # Apply the retry rules to every https:// request made by this session
    session.mount("https://", adapter)

    return session



def main(context=None):
    """Ingest collision records in batches within a time budget."""

    start_time = time.monotonic()
    watermark = load_watermark()

    total_records = 0
    batch_number = 0

    print(f"Starting watermark: {watermark}")

    while time.monotonic() - start_time < MAX_INGESTION_SECONDS:

        # Check how much execution time Lambda has left.
        if context is not None:
            remaining_seconds = (
                context.get_remaining_time_in_millis() / 1000
            )

            # Stop before starting another batch if time is low.
            if remaining_seconds < 60:
                print(
                    "Stopping ingestion: "
                    f"only {remaining_seconds:.1f}s remaining"
                )
                break

        # Fetch the next 100 records after the current watermark.
        records = fetch_records(watermark)

        # A short batch means we reached the current end of the API.
        if not records:
            print("No new records found")
            break

        # Validate before saving anything.
        new_watermark = validate_records(records, watermark)

        # Store the raw data before advancing the checkpoint.
        upload_to_s3(records, new_watermark)

        # Only advance the checkpoint after a successful upload.
        save_watermark(new_watermark)

        # Update our local progress for the next API request.
        watermark = new_watermark
        total_records += len(records)
        batch_number += 1

        print(
            f"Batch {batch_number} completed: "
            f"{len(records)} records"
        )
        print(f"Updated watermark: {watermark}")

        # Fewer than 100 records means no more pages right now.
        if len(records) < BATCH_SIZE:
            print("Caught up with the API")
            break

    print(
        f"Ingestion finished: {total_records} records "
        f"across {batch_number} batches"
    )

# Run main() only when this file is executed directly,
# not when it's imported by another module
if __name__ == "__main__":
    main()

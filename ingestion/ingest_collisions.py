"""
P1: Incremental Data Ingestion.

Fetches collision records in batches, validates them (duplicate check),
uploads raw JSON data to S3 Bronze, and updates a local watermark.

Uses (crash_date, collision_id) as a checkpoint.
collision_id is an opaque hash, so crash_date gives the ordering and
collision_id breaks ties between crashes on the same date.

Usage (run from the repo root so data/state/ resolves correctly):
    BRONZE_BUCKET=<bucket-name> python ingestion/ingest_collisions.py
"""

import json
import os
from pathlib import Path
from datetime import datetime, timezone

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Configuration
# NYC Open Data API endpoint for Motor Vehicle Collisions
API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"


# How many records to request in a single API call
BATCH_SIZE = 100
# Upper limit on records collected in one run (keeps runs small)
MAX_RECORDS = 500

# Set S3 bucket for raw data, read from an environment variable
# (the script fails immediately if BRONZE_BUCKET is not set)
BUCKET_NAME = os.environ["BRONZE_BUCKET"]

# Local file that remembers where the last run stopped
# (kept separate from the checkpoint used in the earlier learning exercise)
WATERMARK_FILE = Path("data/state/ingestion_watermark.json")

# Starting point for the very first run, before any watermark file exists.
# The empty collision_id means "every collision on this date counts as new".
INITIAL_WATERMARK = ("2026-10-01T00:00:00.000", "")


def load_watermark():
    """Reads the last saved (crash_date, collision_id).

    The watermark marks the last record we already ingested.
    If no watermark file exists yet, start from INITIAL_WATERMARK.
    """

    if WATERMARK_FILE.exists():
        with open(WATERMARK_FILE, "r") as file:
            state = json.load(file)
            return (state["last_crash_date"], state["last_collision_id"])

    return INITIAL_WATERMARK


def save_watermark(watermark):
    """Save the checkpoint after a successful upload.

    The next run will pick up from this point, so records
    are not fetched twice.
    """

    # Create the data/state/ folder if it doesn't exist yet
    WATERMARK_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(WATERMARK_FILE, "w") as file:
        json.dump(
            {
                "last_crash_date": watermark[0],
                "last_collision_id": watermark[1]
            },
            file
        )


def fetch_records(watermark):
    """Fetch up to MAX_RECORDS using pagination.

    Asks the API only for records that come after the watermark,
    one batch at a time, and returns them all in one list.
    """
    session = create_session()

    last_date, last_id = watermark
    all_records = []

    # offset = how many records to skip: 0, 100, 200, ...
    for offset in range(0, MAX_RECORDS, BATCH_SIZE):

        # Query parameters in the API's query language (SoQL)
        params = {
            # Only records newer than the watermark: a later date, or
            # the same date with a larger collision_id
            "$where": (
                f"crash_date > '{last_date}' OR "
                f"(crash_date = '{last_date}' "
                f"AND collision_id > '{last_id}')"
            ),
            # A fixed sort order keeps pages from overlapping or skipping rows
            "$order": "crash_date ASC, collision_id ASC",
            "$limit": BATCH_SIZE,
            "$offset": offset
        }
        # make a call to the server.
        response = session.get(
            API_URL,
            params=params,
            timeout=30
        )

        # Stop with an error if the API returned a failure (e.g. 404, 500)
        response.raise_for_status()

        records = response.json()
        all_records.extend(records)

        print(
            f"Fetched {len(records)} records "
            f"at offset {offset}"
        )

        # A short batch means there's no more data to fetch (if data < 100 = batch size)
        if len(records) < BATCH_SIZE:
            break

    return all_records


def validate_records(records, watermark):
    """Validate uniqueness, ordering, and checkpoint of data.

    Raises an error if anything looks wrong. Otherwise returns
    the key of the last record, which becomes the new watermark.
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
    """Upload raw collision records to Bronze S3.

    Saves all records as one JSON file, organized into folders
    by the date the ingestion ran.
    """

    s3 = boto3.client("s3")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # File path inside the bucket, e.g.
    # collisions/ingestion_date=2026-10-31/collisions_2026-10-05_<id>.json
    # ([:10] keeps just the YYYY-MM-DD part of the crash date)
    object_key = (
        f"collisions/ingestion_date={today}/"
        f"collisions_{last_key[0][:10]}_{last_key[1]}.json"
    )

    # Convert the records to JSON text, then to bytes for upload
    json_data = json.dumps(records).encode("utf-8")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
        Body=json_data,
        ContentType="application/json"
    )

    print(f"Uploaded to s3://{BUCKET_NAME}/{object_key}")



# Exponential Backoff
# Increasing the delay between tries.
# It is for the situations when the API fails to give data on requests.
def create_session():
    """Create an HTTP session with automatic retries.

    Failed GET requests are retried up to 3 times, waiting
    longer before each retry (exponential backoff).
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


def main():
    """Run the complete ingestion pipeline.

    Steps: load checkpoint (starting datapoint) -> fetch new records (in batches) 
    -> validate -> upload to S3 -> save new checkpoint (will be used for future runs)
    """

    watermark = load_watermark()

    print(f"Starting watermark: {watermark}")

    records = fetch_records(watermark)

    # Nothing new since the last run, so there's nothing to do
    if not records:
        print("No new records found")
        return

    new_watermark = validate_records(records, watermark)

    # Persist data before advancing the checkpoint.
    # If the upload fails, the watermark stays put and the
    # next run retries the same records, so no data is lost.
    upload_to_s3(records, new_watermark)

    save_watermark(new_watermark)

    print(f"Updated watermark: {new_watermark}")
    print(f"Successfully ingested {len(records)} records")


# Run main() only when this file is executed directly,
# not when it's imported by another module
if __name__ == "__main__":
    main()

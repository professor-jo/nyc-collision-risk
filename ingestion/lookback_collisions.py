'''
This file is to retrieve late-arriving collision records from the NYC Data.


late-arriving records = records that were recorded aafter its actual date.


1. A collision happened on January 1st.
2. BUT the collision was recorded on NYC data on January 7th.
3. We fetch the collision records on January 8th.
4. This file will fetch late-arriving records recorded between Jan 1st ~ 7th
5. Therefore late-arriving records also get recorded and won't get filtered out.
'''

from datetime import datetime, timedelta
import requests
from ingestion.config import API_URL, BATCH_SIZE, MAX_RECORDS
import boto3
import hashlib
import json
import os
from botocore.exceptions import ClientError

def calculate_lookback_start(last_crash_date, days=7):
    '''
    Calculate the start of a historical lookback window.

    Example:
        Last processed crash date = October 8th
        Lookback period = 7 days
        Lookback start = October 1st
    '''

    # Convert the timestamp string into a datetime object
    watermark_date = datetime.fromisoformat(last_crash_date)

    # Subtract the lookback period from the watermark date
    lookback_start = watermark_date - timedelta(days=days)

    # Convert the datetime back into an API-compatible string.
    return lookback_start.isoformat(timespec="milliseconds")




def fetch_lookback_records(start_date, end_date):
    """
    Fetch historical collisions within a date range,
    using pagination until no more records remain.
    """

    all_records = []
    offset = 0

    while True:
        params = {
            "$where": (
                f"crash_date >= '{start_date}' "
                f"AND crash_date < '{end_date}'"
            ),
            "$order": "crash_date ASC, collision_id ASC",
            "$limit": BATCH_SIZE,
            "$offset": offset
        }

        response = requests.get(
            API_URL,
            params=params,
            timeout=30
        )

        response.raise_for_status()
        records = response.json()

        all_records.extend(records)

        print(f"Fetched {len(records)} records at offset {offset}")

        if len(records) < BATCH_SIZE:
            break

        offset += BATCH_SIZE

    return all_records



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

    object_key = (
        f"collisions/lookback/"
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

############################## Running the script ##########################################

if __name__ == "__main__":

    last_crash_date = "2026-10-08T00:00:00.000"

    start_date = calculate_lookback_start(
        last_crash_date,
        days=7
    )

    end_date = "2026-10-09T00:00:00.000"

    # Fetch historical records within the lookback window
    records = fetch_lookback_records(start_date, end_date)

    # Print the lookback window and the number of records fetched
    print(f"Lookback start: {start_date}")
    print(f"Lookback end: {end_date}")
    print(f"Fetched {len(records)} records")

    # Upload the fetched historical records to S3 Bronze
    upload_lookback_to_s3(records, start_date, end_date)
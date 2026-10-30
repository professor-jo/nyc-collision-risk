import json
import os
from datetime import datetime,timezone

import boto3
import requests

# NYC collision data API
API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

# Get the bronze bucket name from an environment variable
BUCKET_NAME = os.environ["BRONZE_BUCKET"]

# Fetch 10 sample records
params = {
    "$limit": 10,
    "$order": "collision_id ASC"
}

# prepare and fetch the API return status (gotta return 200)
response = requests.get(API_URL, params=params, timeout=30)
response.raise_for_status()

# 
records = response.json()
print(f"Fetched {len(records)} records")


# Convert Python records into JSON bytes
json_data = json.dumps(records).encode("utf-8")

# Create an S3 object key
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

object_key = (
    f"collisions/ingestion_date={today}/"
    "sample_collisions.json"
)

# Connect to Amazon S3
s3 = boto3.client("s3")

# Upload the records
s3.put_object(
    Bucket = BUCKET_NAME,
    Key=object_key,
    Body=json_data,
    ContentType="application/json"
)

print("Upload successful!")
print(f"S3 URI: s3://{BUCKET_NAME}/{object_key}")
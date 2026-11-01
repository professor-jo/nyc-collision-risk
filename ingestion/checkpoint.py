import json
import os

import boto3
from botocore.exceptions import ClientError

# S3 bucket used to store the checkpoint.
BUCKET_NAME = os.environ["BRONZE_BUCKET"]

# Fixed location of the checkpoint inside the bucket
CHECKPOINT_KEY = "state/ingestion_watermark.json"

# Starting point when nocheckpoint exists.
INITIAL_WATERMARK = ("2026-10-01T00:00:00.000", "")



def load_watermark():
    '''
    Read the last processed collision from S3.

    If the checkpoint does not exist, 
    use the initial watermark.
    '''

    s3 = boto3.client("s3")

    try:
        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=CHECKPOINT_KEY
        )

    except ClientError as error:
        error_code = error.response["Error"]["Code"]

        if error_code in ("NoSuchKey", "NotFound"):
            print("Checkpoint does not exist. Using initial watermark.")
            return INITIAL_WATERMARK

        raise

    state = json.loads(
        response["Body"].read().decode("utf-8")
    )

    return (
        state["last_crash_date"],
        state["last_collision_id"]
    )

def save_watermark(watermark):
    '''
    Save the new checkpoint to S3.
    '''

    s3 = boto3.client("s3")

    state = {
        "last_crash_date": watermark[0],
        "last_collision_id": watermark[1]
    }

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=CHECKPOINT_KEY,
        Body=json.dumps(state, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    print("Watermark is saved to S3.")


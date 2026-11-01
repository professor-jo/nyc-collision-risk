import json
import os

import boto3
from botocore.exceptions import ClientError

# S3 bucket used to store the checkpoint.
BUCKET_NAME = os.environ["BRONZE_BUCKET"]

# Fixed location of the checkpoint inside the bucket
CHECKPOINT_KEY = "state/ingestion_watermark.json"

# Starting point written by initialize_watermark() for a new environment.
INITIAL_WATERMARK = ("2026-10-01T00:00:00.000", "")


class CheckpointMissingError(RuntimeError):
    '''
    The checkpoint is not in S3, so the pipeline doesn't know where to resume.
    '''


def load_watermark():
    '''
    Read the last processed collision from S3.

    If the checkpoint does not exist, raise an error instead of
    silently restarting historical ingestion.
    '''

    s3 = boto3.client("s3")

    try:
        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=CHECKPOINT_KEY
        )

    except ClientError as error:
        error_code = error.response["Error"]["Code"]

        if error_code in ("404", "NoSuchKey", "NotFound"):
            raise CheckpointMissingError(
                f"No checkpoint at s3://{BUCKET_NAME}/{CHECKPOINT_KEY}. "
                "If this is a new environment, create it with: "
                "python -m ingestion.checkpoint. "
                "Otherwise restore it from an older S3 version."
            ) from error

        raise

    state = json.loads(
        response["Body"].read().decode("utf-8")
    )

    return (
        state["last_crash_date"],
        state["last_collision_id"]
    )


def serialize_watermark(watermark):
    '''
    Convert a watermark into the JSON stored in S3.
    '''

    state = {
        "last_crash_date": watermark[0],
        "last_collision_id": watermark[1]
    }

    return json.dumps(state, indent=2).encode("utf-8")


def save_watermark(watermark):
    '''
    Save the new checkpoint to S3.
    '''

    s3 = boto3.client("s3")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=CHECKPOINT_KEY,
        Body=serialize_watermark(watermark),
        ContentType="application/json"
    )

    print("Watermark is saved to S3.")


def initialize_watermark():
    '''
    Create the first checkpoint for a new environment.

    IfNoneMatch makes S3 reject the write if a checkpoint already exists,
    so this can never rewind an established pipeline.
    '''

    s3 = boto3.client("s3")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=CHECKPOINT_KEY,
        Body=serialize_watermark(INITIAL_WATERMARK),
        ContentType="application/json",
        IfNoneMatch="*"
    )

    print(f"Checkpoint created at {INITIAL_WATERMARK}.")


# Run with: BRONZE_BUCKET=<bucket-name> python -m ingestion.checkpoint
if __name__ == "__main__":
    initialize_watermark()

'''
Tests that the ingestion pipeline is safe to re-run after a failure.

- test_checkpoint_failure: if saving the watermark fails after the S3 upload,
  the error is raised so the batch gets retried instead of skipped.
- test_duplicate_upload_skipped: if the same batch is already in S3,
  it is not uploaded again.

S3, the API and the watermark are all mocked.
'''

from unittest.mock import patch, call
import pytest

from ingestion import ingest_collisions


def test_checkpoint_failure():
    '''
    Simulate a crash after S3 upload but before checkpoint saving.

    Expected:
    - S3 upload is attempted
    - Checkpoint saving fails
    - Pipeline propagates the failure
    '''

    # Create a fake existing watermark
    old_watermark = ("2026-10-05T00:00:00.000", "abc123")

    # Create a fake new watermark.
    new_watermark = ("2026-10-06T00:00:00.000", "xyz789")

    # Create a small fake collision batch.
    records = [
        {
            "crash_date": "2026-10-06T00:00:00.000",
            "collision_id": "xyz789"
        }
    ]

    # Replace real functions with controlled fake versions.
    with (
        patch.object(
            ingest_collisions,
            "load_watermark",
            return_value=old_watermark
        ),
        patch.object(
            ingest_collisions,
            "fetch_records",
            return_value=records
        ),
        patch.object(
            ingest_collisions,
            "upload_to_s3"
        ) as mock_upload,
        patch.object(
            ingest_collisions,
            "save_watermark",
            side_effect=OSError("Simulated checkpoint failure")
        ) as mock_save,
    ):

# The pipeline should fail when checkpoint saving fails.
        with pytest.raises(
            OSError,
            match="Simulated checkpoint failure"
        ):
            ingest_collisions.main()

        # Confirm S3 upload was attempted exactly once.
        mock_upload.assert_called_once_with(
            records,
            new_watermark
        )

        # Confirm the pipeline attempted to save the checkpoint.
        mock_save.assert_called_once_with(new_watermark)



def test_duplicate_upload_skipped():
    """
    Verify that an identical batch already stored in S3
    is not uploaded a second time.
    """

    import json
    import hashlib

    # Create a sample collision batch.
    records = [
        {
            "crash_date": "2026-10-06T00:00:00.000",
            "collision_id": "xyz789"
        }
    ]

    last_key = (
        "2026-10-06T00:00:00.000",
        "xyz789"
    )

    # Serialize the records exactly as our ingestion code does.
    json_data = json.dumps(
        records,
        sort_keys=True,
        separators=(",", ":")
    ).encode("utf-8")

    # Calculate the checksum for our fake batch.
    checksum = hashlib.sha256(json_data).hexdigest()

    # Replace boto3.client() with a fake S3 client.
    with patch.object(
        ingest_collisions.boto3,
        "client"
    ) as mock_client:

        mock_s3 = mock_client.return_value

        # Pretend S3 already contains an object
        # with the exact same SHA-256 checksum.
        mock_s3.head_object.return_value = {
            "Metadata": {
                "sha256": checksum
            }
        }

        # Run the REAL upload function against the fake S3 client.
        ingest_collisions.upload_to_s3(records, last_key)

        # Confirm that S3 was checked for an existing object.
        mock_s3.head_object.assert_called_once()

        # Confirm that no new upload was performed.
        mock_s3.put_object.assert_not_called()


##########################################################################################

def main():
    # Step 1: Load the previous checkpoint
    watermark = load_watermark()

    # Step 2: Fetch records after that checkpoint
    records = fetch_records(watermark)

    # Step 3: Stop if no new records exist
    if not records:
        print("No new records found")
        return

    # Step 4: Validate records and determine new checkpoint
    new_watermark = validate_records(records, watermark)

    # Step 5: Upload data to S3 first
    upload_to_s3(records, new_watermark)

    # Step 6: Save checkpoint only after successful upload
    save_watermark(new_watermark)


if __name__ == "__main__":
    main()
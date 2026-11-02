'''
Tests that the ingestion pipeline is safe to re-run after a failure.


Current data pipeline:

    1. Load existing watermark
    2. Fetch new records from API
    3. Validate records
    4. Check S3 (idempotency: if same batch exists / conflicting contents) and upload if needed.
    5. Save the new watermark (only after S3 confirms the batch is stored)


- test_checkpoint_failure: 
    if saving the watermark fails after the S3 upload,
    the error is raised so the batch gets retried instead of skipped.

- test_duplicate_upload_skipped: 
    if the same batch is already in S3, it is not uploaded again.

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
    old_watermark = ("2026-10-05T00:00:00.000", "fdbdc3a754bcb7538295548469bc4631")

    # Create a fake new watermark.
    new_watermark = ("2026-10-06T00:00:00.000", "473b14600b6e9df374c28da7f9613c8a")

    # Create a small fake collision batch.
    records = [
        {
            "crash_date": "2026-10-06T00:00:00.000",
            "collision_id": "473b14600b6e9df374c28da7f9613c8a"
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

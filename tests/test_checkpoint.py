'''
Tests reading, writing and creating the S3 checkpoint.

- A missing checkpoint stops the pipeline instead of restarting ingestion.
- Other S3 failures and unreadable checkpoints are raised, not hidden.
- Creating the first checkpoint can't overwrite an existing one.

S3 is mocked, so nothing here touches AWS.
'''

import io
import json
from unittest.mock import patch

from botocore.exceptions import ClientError
import pytest

from ingestion import checkpoint, ingest_collisions


WATERMARK = ("2026-10-06T00:00:00.000", "xyz789")

STATE = {
    "last_crash_date": "2026-10-06T00:00:00.000",
    "last_collision_id": "xyz789"
}


def client_error(code, operation="GetObject"):
    '''
    Build the error boto3 raises when S3 rejects a request.
    '''

    return ClientError({"Error": {"Code": code}}, operation)


def s3_body(content):
    '''
    Build the response S3 returns from get_object.
    '''

    return {"Body": io.BytesIO(content.encode("utf-8"))}


@pytest.fixture
def mock_s3():
    '''
    Replace boto3.client() with a fake S3 client.
    '''

    with patch.object(checkpoint.boto3, "client") as mock_client:
        yield mock_client.return_value


def test_load_returns_stored_watermark(mock_s3):
    mock_s3.get_object.return_value = s3_body(json.dumps(STATE))

    assert checkpoint.load_watermark() == WATERMARK

    mock_s3.get_object.assert_called_once_with(
        Bucket=checkpoint.BUCKET_NAME,
        Key=checkpoint.CHECKPOINT_KEY
    )


@pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
def test_load_missing_checkpoint_raises(mock_s3, code):
    mock_s3.get_object.side_effect = client_error(code)

    with pytest.raises(checkpoint.CheckpointMissingError):
        checkpoint.load_watermark()


def test_load_other_s3_error_is_not_treated_as_missing(mock_s3):
    mock_s3.get_object.side_effect = client_error("AccessDenied")

    with pytest.raises(ClientError, match="AccessDenied"):
        checkpoint.load_watermark()


def test_load_invalid_json_raises(mock_s3):
    mock_s3.get_object.return_value = s3_body("not json")

    with pytest.raises(json.JSONDecodeError):
        checkpoint.load_watermark()


def test_load_missing_field_raises(mock_s3):
    mock_s3.get_object.return_value = s3_body(
        json.dumps({"last_crash_date": "2026-10-06T00:00:00.000"})
    )

    with pytest.raises(KeyError, match="last_collision_id"):
        checkpoint.load_watermark()


def test_save_writes_watermark_to_checkpoint_key(mock_s3):
    checkpoint.save_watermark(WATERMARK)

    mock_s3.put_object.assert_called_once()
    request = mock_s3.put_object.call_args.kwargs

    assert request["Bucket"] == checkpoint.BUCKET_NAME
    assert request["Key"] == checkpoint.CHECKPOINT_KEY
    assert request["ContentType"] == "application/json"
    assert json.loads(request["Body"]) == STATE

    # A normal save must be able to replace the previous checkpoint.
    assert "IfNoneMatch" not in request


def test_saved_checkpoint_can_be_loaded_back(mock_s3):
    checkpoint.save_watermark(WATERMARK)

    body = mock_s3.put_object.call_args.kwargs["Body"]
    mock_s3.get_object.return_value = {"Body": io.BytesIO(body)}

    assert checkpoint.load_watermark() == WATERMARK


def test_save_failure_raises(mock_s3):
    mock_s3.put_object.side_effect = client_error("InternalError", "PutObject")

    with pytest.raises(ClientError, match="InternalError"):
        checkpoint.save_watermark(WATERMARK)


def test_initialize_only_writes_if_no_checkpoint_exists(mock_s3):
    checkpoint.initialize_watermark()

    request = mock_s3.put_object.call_args.kwargs

    assert request["Key"] == checkpoint.CHECKPOINT_KEY
    assert request["IfNoneMatch"] == "*"
    assert json.loads(request["Body"]) == {
        "last_crash_date": checkpoint.INITIAL_WATERMARK[0],
        "last_collision_id": checkpoint.INITIAL_WATERMARK[1]
    }


def test_initialize_refuses_to_overwrite_existing_checkpoint(mock_s3):
    mock_s3.put_object.side_effect = client_error(
        "PreconditionFailed",
        "PutObject"
    )

    with pytest.raises(ClientError, match="PreconditionFailed"):
        checkpoint.initialize_watermark()


def test_pipeline_stops_before_fetching_when_checkpoint_is_missing(mock_s3):
    mock_s3.get_object.side_effect = client_error("NoSuchKey")

    with patch.object(ingest_collisions, "fetch_records") as mock_fetch:
        with pytest.raises(checkpoint.CheckpointMissingError):
            ingest_collisions.main()

    mock_fetch.assert_not_called()
    mock_s3.put_object.assert_not_called()

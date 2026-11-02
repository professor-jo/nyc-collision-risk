
import pytest
from unittest.mock import patch

from ingestion.lambda_handler import lambda_handler


'''
This file tests the following:

test_successful_ingestion tests if
	- Acquire lock → ingest → release lock
test_existing_lock tests if
    - Skip ingestion when lock is held
test_ingestion_failure tests if
    - test_Release lock even when ingestion fails

'''

@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_successful_ingestion(mock_acquire, mock_main, mock_release):
    mock_acquire.return_value = "owner-123"

    result = lambda_handler({}, None)

    assert result["statusCode"] == 200
    mock_main.assert_called_once()
    mock_release.assert_called_once_with("owner-123")


@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_existing_lock(mock_acquire, mock_main, mock_release):
    mock_acquire.return_value = None

    result = lambda_handler({}, None)

    assert result["statusCode"] == 409
    mock_main.assert_not_called()
    mock_release.assert_not_called()


@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_ingestion_failure(mock_acquire, mock_main, mock_release):
    mock_acquire.return_value = "owner-123"
    mock_main.side_effect = RuntimeError("API failure")

    with pytest.raises(RuntimeError, match="API failure"):
        lambda_handler({}, None)

    mock_release.assert_called_once_with("owner-123")

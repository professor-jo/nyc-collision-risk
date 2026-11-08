
import pytest
from unittest.mock import Mock, call, patch

from ingestion.lambda_handler import lambda_handler


'''
This file tests the following:

test_successful_ingestion tests if
	- Acquire lock → ingest → release lock
test_existing_lock tests if
    - Skip ingestion when lock is held
test_ingestion_failure tests if
    - test_Release lock even when ingestion fails
test_lookback_failure tests if
    - Release lock and report the failure when the lookback fails

'''

@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.run_lookback")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_successful_ingestion(mock_acquire, mock_main, mock_lookback, mock_release):
    mock_acquire.return_value = "owner-123"

    result = lambda_handler({}, None)

    assert result["statusCode"] == 200
    mock_main.assert_called_once()
    mock_lookback.assert_called_once()
    mock_release.assert_called_once_with("owner-123")


@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.run_lookback")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_existing_lock(mock_acquire, mock_main, mock_lookback, mock_release):
    mock_acquire.return_value = None

    result = lambda_handler({}, None)

    assert result["statusCode"] == 409
    mock_main.assert_not_called()
    mock_lookback.assert_not_called()
    mock_release.assert_not_called()


@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.run_lookback")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_ingestion_failure(mock_acquire, mock_main, mock_lookback, mock_release):
    mock_acquire.return_value = "owner-123"
    mock_main.side_effect = RuntimeError("API failure")

    with pytest.raises(RuntimeError, match="API failure"):
        lambda_handler({}, None)

    mock_lookback.assert_not_called()
    mock_release.assert_called_once_with("owner-123")


@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.run_lookback")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_lookback_failure(mock_acquire, mock_main, mock_lookback, mock_release):
    mock_acquire.return_value = "owner-123"
    mock_lookback.side_effect = RuntimeError("API failure")

    with pytest.raises(RuntimeError, match="API failure"):
        lambda_handler({}, None)

    mock_release.assert_called_once_with("owner-123")


@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.run_lookback")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_incremental_runs_before_lookback(mock_acquire, mock_main, mock_lookback, mock_release):
    mock_acquire.return_value = "owner-123"

    # Return plain dicts, like the real functions. Reading keys from a
    # MagicMock result would be recorded as extra calls below.
    mock_main.return_value = {"caught_up": True, "bronze_changed": False}
    mock_lookback.return_value = {
        "bronze_changed": False,
        "lookback_complete": True,
    }

    steps = Mock()
    steps.attach_mock(mock_main, "main")
    steps.attach_mock(mock_lookback, "lookback")
    context = Mock()

    lambda_handler({}, context)

    assert steps.mock_calls == [
        call.main(context=context),
        call.lookback(context=context),
    ]


@pytest.mark.parametrize(
    "incremental_changed, lookback_changed, expected",
    [
        (False, False, False),
        (True, False, True),
        (False, True, True),
        (True, True, True),
    ],
)
@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.run_lookback")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_result_combines_incremental_and_lookback(
    mock_acquire,
    mock_main,
    mock_lookback,
    mock_release,
    incremental_changed,
    lookback_changed,
    expected,
):
    """Bronze changed if either the incremental run or the lookback wrote."""
    mock_acquire.return_value = "owner-123"
    mock_main.return_value = {
        "caught_up": False,
        "bronze_changed": incremental_changed,
    }
    mock_lookback.return_value = {
        "bronze_changed": lookback_changed,
        "lookback_complete": True,
    }

    result = lambda_handler({}, None)

    assert result["bronze_changed"] is expected
    assert result["caught_up"] is False
    assert result["lookback_complete"] is True

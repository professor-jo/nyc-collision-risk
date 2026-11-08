
import pytest
from unittest.mock import Mock, call, patch

from ingestion.lambda_handler import IngestionLockHeld, lambda_handler


'''
This file tests the following:

test_successful_ingestion tests if
	- Acquire lock → ingest → release lock
test_existing_lock tests if
    - Raise IngestionLockHeld and skip ingestion when lock is held
test_ingestion_failure tests if
    - test_Release lock even when ingestion fails
test_lookback_failure tests if
    - Release lock and report an incomplete lookback when the lookback fails

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

    with pytest.raises(IngestionLockHeld):
        lambda_handler({}, None)

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
def test_lookback_failure(mock_acquire, mock_main, mock_lookback, mock_release, capsys):
    mock_acquire.return_value = "owner-123"
    mock_main.return_value = {"caught_up": True, "bronze_changed": True}
    mock_lookback.side_effect = RuntimeError("API failure")

    result = lambda_handler({}, None)

    # What ingestion wrote is still reported, so Silver isn't skipped.
    assert result == {
        "statusCode": 200,
        "caught_up": True,
        "bronze_changed": True,
        "lookback_ran": True,
        "lookback_complete": False,
    }
    # The three-day CloudWatch alarm counts this exact text.
    assert "LOOKBACK_STATUS=incomplete" in capsys.readouterr().out
    mock_release.assert_called_once_with("owner-123")


@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.run_lookback")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_lookback_failure_does_not_claim_bronze_changed(
    mock_acquire, mock_main, mock_lookback, mock_release
):
    mock_acquire.return_value = "owner-123"
    mock_main.return_value = {"caught_up": True, "bronze_changed": False}
    mock_lookback.side_effect = RuntimeError("API failure")

    result = lambda_handler({}, None)

    assert result["bronze_changed"] is False
    assert result["lookback_complete"] is False


@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.run_lookback")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_incomplete_lookback_keeps_ingestion_result(
    mock_acquire, mock_main, mock_lookback, mock_release
):
    # The lookback ran out of time and returned normally, without raising.
    mock_acquire.return_value = "owner-123"
    mock_main.return_value = {"caught_up": True, "bronze_changed": True}
    mock_lookback.return_value = {
        "bronze_changed": False,
        "lookback_complete": False,
    }

    result = lambda_handler({}, None)

    assert result["bronze_changed"] is True
    assert result["lookback_complete"] is False


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


def run_handler(event, caught_up, bronze_changed=True):
    """Run the handler with ingestion and the lock mocked out."""
    with (
        patch("ingestion.lambda_handler.acquire_lock", return_value="owner-123"),
        patch("ingestion.lambda_handler.release_lock"),
        patch(
            "ingestion.lambda_handler.main",
            return_value={
                "caught_up": caught_up,
                "bronze_changed": bronze_changed,
            },
        ),
        patch(
            "ingestion.lambda_handler.run_lookback",
            return_value={"bronze_changed": False, "lookback_complete": True},
        ) as mock_lookback,
    ):
        return lambda_handler(event, None), mock_lookback


def test_lookback_skipped_while_still_catching_up():
    result, mock_lookback = run_handler({"attempt": 1}, caught_up=False)

    mock_lookback.assert_not_called()
    assert result == {
        "statusCode": 200,
        "caught_up": False,
        "bronze_changed": True,
        "lookback_ran": False,
        "lookback_complete": None,
    }


def test_lookback_runs_on_last_attempt_even_if_not_caught_up():
    result, mock_lookback = run_handler({"attempt": 5}, caught_up=False)

    mock_lookback.assert_called_once()
    assert result["caught_up"] is False
    assert result["lookback_ran"] is True
    assert result["lookback_complete"] is True


def test_lookback_runs_as_soon_as_ingestion_catches_up():
    result, mock_lookback = run_handler({"attempt": 2}, caught_up=True)

    mock_lookback.assert_called_once()
    assert result["lookback_ran"] is True


def test_lookback_runs_when_no_attempt_is_passed():
    # The EventBridge schedule and manual invokes don't send an attempt.
    result, mock_lookback = run_handler({}, caught_up=False)

    mock_lookback.assert_called_once()
    assert result["lookback_ran"] is True


@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.run_backfill")
@patch("ingestion.lambda_handler.run_lookback")
@patch("ingestion.lambda_handler.main")
@patch("ingestion.lambda_handler.acquire_lock")
def test_backfill_runs_only_its_date_range(
    mock_acquire, mock_main, mock_lookback, mock_backfill, mock_release
):
    mock_acquire.return_value = "owner-123"
    mock_backfill.return_value = {"bronze_changed": True}
    context = Mock()

    result = lambda_handler(
        {
            "mode": "backfill",
            "backfill_start": "2026-10-01",
            "backfill_end": "2026-10-15",
        },
        context,
    )

    assert result == {
        "statusCode": 200,
        "caught_up": True,
        "bronze_changed": True,
        "lookback_ran": False,
        "lookback_complete": None,
    }
    mock_backfill.assert_called_once_with(
        "2026-10-01", "2026-10-15", context=context
    )
    mock_main.assert_not_called()
    mock_lookback.assert_not_called()
    mock_release.assert_called_once_with("owner-123")


@patch("ingestion.lambda_handler.release_lock")
@patch("ingestion.lambda_handler.run_backfill")
@patch("ingestion.lambda_handler.acquire_lock")
def test_backfill_failure_releases_lock_and_raises(
    mock_acquire, mock_backfill, mock_release
):
    mock_acquire.return_value = "owner-123"
    mock_backfill.side_effect = RuntimeError("API failure")

    with pytest.raises(RuntimeError, match="API failure"):
        lambda_handler(
            {
                "mode": "backfill",
                "backfill_start": "2026-10-01",
                "backfill_end": "2026-10-15",
            },
            None,
        )

    mock_release.assert_called_once_with("owner-123")

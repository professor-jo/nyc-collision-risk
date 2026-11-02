
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from ingestion.lock import acquire_lock, release_lock


def conditional_failure():
    return ClientError(
        {
            "Error": {
                "Code": "ConditionalCheckFailedException",
                "Message": "Condition failed",
            }
        },
        "PutItem",
    )


@patch("ingestion.lock.get_lock_table")
def test_acquire_lock_success(mock_get_table):
    table = MagicMock()
    mock_get_table.return_value = table

    owner_id = acquire_lock()

    assert isinstance(owner_id, str)
    assert owner_id
    table.put_item.assert_called_once()


@patch("ingestion.lock.get_lock_table")
def test_acquire_lock_already_held(mock_get_table):
    table = MagicMock()
    mock_get_table.return_value = table
    table.put_item.side_effect = conditional_failure()

    assert acquire_lock() is None


@patch("ingestion.lock.get_lock_table")
def test_release_lock_success(mock_get_table):
    table = MagicMock()
    mock_get_table.return_value = table

    release_lock("owner-123")

    table.delete_item.assert_called_once()
    assert table.delete_item.call_args.kwargs[
        "ExpressionAttributeValues"
    ][":owner"] == "owner-123"


@patch("ingestion.lock.get_lock_table")
def test_release_lock_wrong_owner(mock_get_table):
    table = MagicMock()
    mock_get_table.return_value = table
    table.delete_item.side_effect = conditional_failure()

    release_lock("wrong-owner")
    table.delete_item.assert_called_once()

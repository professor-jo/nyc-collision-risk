'''
We simulate 3 API responses,

Request 1 (100 records)
Upload batch 1 → Save checkpoint 1

Request 2 (100 records)
Upload batch 2 → Save checkpoint 2

Request 3 (0 records)
Stop ingestion

'''
from unittest.mock import patch, call

from ingestion import ingest_collisions


def test_multiple_batches_update_checkpoint():
    """Verify that each batch is saved before fetching the next."""

    old_watermark = ("2026-10-01T00:00:00.000", "fdbdc3a754bcb7538295548469bc4631")
    first_watermark = ("2026-10-02T00:00:00.000", "473b14600b6e9df374c28da7f9613c8a")
    second_watermark = ("2026-10-03T00:00:00.000", "68d7e767240043d851c28d33f8787024")

    # Two full batches, followed by an empty response.
    batch_1 = [{"collision_id": f"{i:032x}"} for i in range(100)]
    batch_2 = [{"collision_id": f"{i:032x}"} for i in range(100, 200)]

    with (
        patch.object(
            ingest_collisions,
            "load_watermark",
            return_value=old_watermark,
        ),
        patch.object(
            ingest_collisions,
            "fetch_records",
            side_effect=[batch_1, batch_2, []],
        ) as mock_fetch,
        patch.object(
            ingest_collisions,
            "validate_records",
            side_effect=[first_watermark, second_watermark],
        ),
        patch.object(
            ingest_collisions,
            "upload_to_s3",
        ) as mock_upload,
        patch.object(
            ingest_collisions,
            "save_watermark",
        ) as mock_save,
    ):
        ingest_collisions.main()

    # Every request must start after the previous checkpoint.
    assert mock_fetch.call_args_list == [
        call(old_watermark),
        call(first_watermark),
        call(second_watermark),
    ]

    # Both batches must be uploaded.
    assert mock_upload.call_args_list == [
        call(batch_1, first_watermark),
        call(batch_2, second_watermark),
    ]

    # Each successful batch must advance the checkpoint.
    assert mock_save.call_args_list == [
        call(first_watermark),
        call(second_watermark),
    ]



def test_ingestion_stops_when_lambda_time_is_low():
    """Stop ingestion when Lambda has fewer than 60 seconds left."""

    from unittest.mock import Mock

    # Simulate an AWS Lambda context with 45 seconds remaining.
    mock_context = Mock()
    mock_context.get_remaining_time_in_millis.return_value = 45000

    with (
        patch.object(
            ingest_collisions,
            "load_watermark",
            return_value=("2026-10-01T00:00:00.000", "fdbdc3a754bcb7538295548469bc4631"),
        ),
        patch.object(
            ingest_collisions,
            "fetch_records",
        ) as mock_fetch,
        patch.object(
            ingest_collisions,
            "upload_to_s3",
        ) as mock_upload,
        patch.object(
            ingest_collisions,
            "save_watermark",
        ) as mock_save,
    ):
        ingest_collisions.main(context=mock_context)

    # No API request should start when Lambda has too little time.
    mock_fetch.assert_not_called()

    # No data should be uploaded or checkpoint advanced.
    mock_upload.assert_not_called()
    mock_save.assert_not_called()



def test_fetch_records_uses_keyset_pagination():
    """Verify that the API query uses the watermark, not offsets."""

    watermark = ("2026-10-01T00:00:00.000", "fdbdc3a754bcb7538295548469bc4631")

    fake_records = [
        {
            "crash_date": "2026-10-02T00:00:00.000",
            "collision_id": "473b14600b6e9df374c28da7f9613c8a",
        }
    ]

    with patch.object(
        ingest_collisions,
        "create_session",
    ) as mock_create_session:

        mock_session = mock_create_session.return_value
        mock_response = mock_session.get.return_value
        mock_response.json.return_value = fake_records

        records = ingest_collisions.fetch_records(watermark)

    # Verify the function returned the API records.
    assert records == fake_records

    # Inspect the actual query parameters sent to the API.
    params = mock_session.get.call_args.kwargs["params"]

    assert "$offset" not in params
    assert params["$limit"] == 100
    assert params["$order"] == "crash_date ASC, collision_id ASC"

    # Verify both components of the watermark are used.
    assert "2026-10-01T00:00:00.000" in params["$where"]
    # collision_id is a text column, so the value must be quoted.
    assert "collision_id > 'fdbdc3a754bcb7538295548469bc4631'" in params["$where"]

    mock_response.raise_for_status.assert_called_once()


def test_ingestion_stops_at_time_budget():
    """Stop starting batches once MAX_INGESTION_SECONDS has passed."""

    watermark = ("2026-10-02T00:00:00.000", "473b14600b6e9df374c28da7f9613c8a")
    batch = [{"collision_id": f"{i:032x}"} for i in range(100)]

    with (
        patch.object(
            ingest_collisions,
            "load_watermark",
            return_value=("2026-10-01T00:00:00.000", "fdbdc3a754bcb7538295548469bc4631"),
        ),
        patch.object(
            ingest_collisions,
            "fetch_records",
            return_value=batch,
        ) as mock_fetch,
        patch.object(
            ingest_collisions,
            "validate_records",
            return_value=watermark,
        ),
        patch.object(ingest_collisions, "upload_to_s3"),
        patch.object(ingest_collisions, "save_watermark") as mock_save,
        patch.object(
            ingest_collisions.time,
            "monotonic",
            side_effect=[0, 0, ingest_collisions.MAX_INGESTION_SECONDS],
        ),
    ):
        ingest_collisions.main()

    # The first full batch finishes, and the next one never starts.
    mock_fetch.assert_called_once()
    mock_save.assert_called_once_with(watermark)


'''
Tests the late-arriving records lookback.

- The window covers the lookback days plus the watermark's own date.
- The API is paged with keyset pagination, limited to the window.
- The ingestion watermark is never saved by the lookback.
- Files are stored by ingestion date, so Silver can pick the latest version.
- A run that stops early uploads nothing, and the next run replays the window.

S3, the API and the watermark are all mocked.
'''

from unittest.mock import Mock, patch

from ingestion import ingest_collisions, lookback_collisions


def make_batch(start, size, crash_date="2026-10-20T00:00:00.000"):
    return [
        {"crash_date": crash_date, "collision_id": f"{i:032x}"}
        for i in range(start, start + size)
    ]


def test_window_includes_lookback_days_and_watermark_date():
    last_crash_date = "2026-10-27T00:00:00.000"

    assert (
        lookback_collisions.calculate_lookback_start(last_crash_date)
        == "2026-10-13T00:00:00.000"
    )
    assert (
        lookback_collisions.calculate_lookback_end(last_crash_date)
        == "2026-10-28T00:00:00.000"
    )


def test_fetch_records_limits_query_to_end_date():
    with patch.object(
        ingest_collisions,
        "create_session",
    ) as mock_create_session:

        mock_session = mock_create_session.return_value
        mock_session.get.return_value.json.return_value = []

        ingest_collisions.fetch_records(
            ("2026-10-13T00:00:00.000", ""),
            end_date="2026-10-28T00:00:00.000",
        )

    where = mock_session.get.call_args.kwargs["params"]["$where"]

    # The end date must apply to both sides of the OR.
    assert where.startswith("crash_date < '2026-10-28T00:00:00.000' AND (")
    assert where.endswith(")")


def test_fetch_lookback_pages_from_last_record():
    start_date = "2026-10-13T00:00:00.000"
    end_date = "2026-10-28T00:00:00.000"
    full_batch = make_batch(0, 100)
    short_batch = make_batch(100, 5)

    with patch.object(
        lookback_collisions,
        "fetch_records",
        side_effect=[full_batch, short_batch],
    ) as mock_fetch:
        records, complete = lookback_collisions.fetch_lookback_records(
            start_date,
            end_date,
        )

    assert records == full_batch + short_batch
    assert complete is True

    positions = [c.args[0] for c in mock_fetch.call_args_list]
    assert positions == [
        (start_date, ""),
        (full_batch[-1]["crash_date"], full_batch[-1]["collision_id"]),
    ]


def test_fetch_lookback_stops_when_lambda_time_is_low():
    mock_context = Mock()
    mock_context.get_remaining_time_in_millis.return_value = 45000

    with patch.object(lookback_collisions, "fetch_records") as mock_fetch:
        records, complete = lookback_collisions.fetch_lookback_records(
            "2026-10-13T00:00:00.000",
            "2026-10-28T00:00:00.000",
            context=mock_context,
        )

    assert records == []
    assert complete is False
    mock_fetch.assert_not_called()


def test_incomplete_run_uploads_nothing_and_next_run_replays_window(capsys):
    start_date = "2026-10-13T00:00:00.000"
    full_batch = make_batch(0, 100)
    short_batch = make_batch(100, 5)

    # One batch fits, then time runs out before the second request.
    low_time_context = Mock()
    low_time_context.get_remaining_time_in_millis.side_effect = [120000, 45000]

    plenty_of_time_context = Mock()
    plenty_of_time_context.get_remaining_time_in_millis.return_value = 280000

    with (
        patch.object(
            lookback_collisions,
            "load_watermark",
            return_value=("2026-10-27T00:00:00.000", "ff" * 16),
        ),
        patch.object(
            lookback_collisions,
            "fetch_records",
            side_effect=[full_batch, full_batch, short_batch],
        ) as mock_fetch,
        patch.object(
            lookback_collisions,
            "upload_lookback_to_s3",
        ) as mock_upload,
    ):
        lookback_collisions.run_lookback(context=low_time_context)

        # The request after the time check never started.
        assert mock_fetch.call_count == 1
        mock_upload.assert_not_called()
        assert "LOOKBACK_STATUS=incomplete" in capsys.readouterr().out

        lookback_collisions.run_lookback(context=plenty_of_time_context)

    # The second run starts again from the beginning of the window.
    assert mock_fetch.call_args_list[1].args[0] == (start_date, "")
    mock_upload.assert_called_once_with(
        full_batch + short_batch,
        start_date,
        "2026-10-28T00:00:00.000",
    )
    assert "LOOKBACK_STATUS=complete" in capsys.readouterr().out


def test_run_lookback_never_saves_the_watermark():
    records = make_batch(0, 5)

    with (
        patch.object(
            lookback_collisions,
            "load_watermark",
            return_value=("2026-10-27T00:00:00.000", "ff" * 16),
        ),
        patch.object(
            lookback_collisions,
            "fetch_records",
            return_value=records,
        ),
        patch.object(
            lookback_collisions,
            "upload_lookback_to_s3",
        ) as mock_upload,
        patch("ingestion.checkpoint.save_watermark") as mock_checkpoint_save,
        patch.object(ingest_collisions, "save_watermark") as mock_ingest_save,
    ):
        lookback_collisions.run_lookback()

    mock_upload.assert_called_once_with(
        records,
        "2026-10-13T00:00:00.000",
        "2026-10-28T00:00:00.000",
    )
    mock_checkpoint_save.assert_not_called()
    mock_ingest_save.assert_not_called()


def test_upload_key_is_partitioned_by_ingestion_date():
    mock_s3 = Mock()
    mock_s3.head_object.side_effect = lookback_collisions.ClientError(
        {"Error": {"Code": "404", "Message": "Not found"}},
        "HeadObject",
    )

    with patch.object(lookback_collisions.boto3, "client", return_value=mock_s3):
        lookback_collisions.upload_lookback_to_s3(
            make_batch(0, 1),
            "2026-10-13T00:00:00.000",
            "2026-10-28T00:00:00.000",
        )

    key = mock_s3.put_object.call_args.kwargs["Key"]

    assert key.startswith("collisions/lookback/ingestion_date=")
    assert "/start_date=2026-10-13/end_date=2026-10-28/batch_" in key

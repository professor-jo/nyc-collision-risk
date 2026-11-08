
from ingestion.config import MAX_INGESTION_ATTEMPTS
from ingestion.ingest_collisions import main
from ingestion.lock import acquire_lock, release_lock
from ingestion.lookback_collisions import run_lookback


class IngestionLockHeld(Exception):
    pass


def lambda_handler(event, context):
    # Try to acquire exclusive access to the ingestion pipeline.
    owner_id = acquire_lock()

    if owner_id is None:
        raise IngestionLockHeld(
            "Another ingestion execution currently holds the lock."
        )

    try:
        # Execute the existing NYC collision ingestion pipeline.
        incremental = main(context=context)

        # Step Functions passes the attempt number. Anything else, such as
        # the EventBridge schedule or a manual invoke, is a single run, so
        # treat it as the final attempt and still run the lookback.
        attempt = event.get("attempt", MAX_INGESTION_ATTEMPTS)

        # The lookback re-fetches 14 days, so run it once per catch-up:
        # when ingestion has caught up, or on the last allowed attempt.
        lookback_ran = (
            incremental["caught_up"]
            or attempt >= MAX_INGESTION_ATTEMPTS
        )

        bronze_changed = incremental["bronze_changed"]
        lookback_complete = None

        if lookback_ran:
            # Re-fetch recent dates for records published after the
            # watermark passed them. This never moves the watermark.
            #
            # A failed lookback must not discard what ingestion already wrote,
            # or Step Functions would skip Silver. The next run replays the
            # same window, so report it as incomplete instead of raising.
            try:
                lookback = run_lookback(context=context)
                bronze_changed = bronze_changed or lookback["bronze_changed"]
                lookback_complete = lookback["lookback_complete"]
            except Exception as error:
                # A CloudWatch alarm in InfraStack watches for this exact text.
                print(f"LOOKBACK_STATUS=incomplete: {error!r}")
                lookback_complete = False

        return {
            "statusCode": 200,
            "caught_up": incremental["caught_up"],
            "bronze_changed": bronze_changed,
            "lookback_ran": lookback_ran,
            "lookback_complete": lookback_complete,
        }

    # Unlike try / except pair which handles errors, 
    # finally runs a cleanup code even when error occurs.
    finally:
        # Release the lock whether ingestion succeeds or fails.
        release_lock(owner_id)

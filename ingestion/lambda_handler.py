
from ingestion.ingest_collisions import main
from ingestion.lock import acquire_lock, release_lock
from ingestion.lookback_collisions import run_lookback


def lambda_handler(event, context):
    # Try to acquire exclusive access to the ingestion pipeline.
    owner_id = acquire_lock()

    if owner_id is None:
        print("Another ingestion execution is running. Skipping.")
        return {
            "statusCode": 409,
            "message": "Ingestion skipped: lock already held",
        }

    try:
        # Execute the existing NYC collision ingestion pipeline.
        incremental = main(context=context)

        # Re-fetch recent dates for records published after the watermark
        # passed them. This never moves the watermark.
        lookback = run_lookback(context=context)

        return {
            "statusCode": 200,
            "bronze_changed": (
                incremental['bronze_changed']
                or lookback['bronze_changed']
            ),
            'caught_up': incremental['caught_up'],
            'lookback_complete': lookback['lookback_complete'],
        }

    # Unlike try / except pair which handles errors, 
    # finally runs a cleanup code even when error occurs.
    finally:
        # Release the lock whether ingestion succeeds or fails.
        release_lock(owner_id)

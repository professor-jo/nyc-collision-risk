
from ingestion.ingest_collisions import main
from ingestion.lock import acquire_lock, release_lock


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
        main(context=context)

        return {
            "statusCode": 200,
            "message": "Collision ingestion completed",
        }

    # Unlike try / except pair which handles errors, 
    # finally runs a cleanup code even when error occurs.
    finally:
        # Release the lock whether ingestion succeeds or fails.
        release_lock(owner_id)

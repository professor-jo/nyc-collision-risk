
from datetime import datetime


REQUIRED_FIELDS = {
    "event_id",
    "event_type",
    "source",
    "observed_at",
    "ingested_at",
    "location",
    "measurements",
}

ALLOWED_EVENT_TYPES = {"traffic", "weather"}


def validate_event(event):
    # Verify every required field is present.
    missing_fields = REQUIRED_FIELDS - event.keys()

    if missing_fields:
        raise ValueError(
            f"Missing required fields: {missing_fields}"
        )

    # Check the event category.
    if event["event_type"] not in ALLOWED_EVENT_TYPES:
        raise ValueError("Unsupported event type")

    # Check the event identifier.
    if not isinstance(event["event_id"], str):
        raise ValueError("Invalid event ID")

    if len(event["event_id"]) != 64:
        raise ValueError("Expected SHA-256 event ID")

    # Verify timestamps are timezone-aware.
    for field in ("observed_at", "ingested_at"):
        timestamp = datetime.fromisoformat(
            event[field].replace("Z", "+00:00")
        )

        if timestamp.tzinfo is None:
            raise ValueError(
                f"{field} must include a timezone"
            )

    # Verify nested fields.
    if not isinstance(event["location"], dict):
        raise ValueError("Location must be an object")

    if not isinstance(event["measurements"], dict):
        raise ValueError("Measurements must be an object")

    return True

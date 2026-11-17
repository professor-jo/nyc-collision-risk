
import hashlib
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def normalize_traffic(record):
    # NYC DOT timestamps may be expressed in NYC local time.
    raw_time = record["data_as_of"]

    parsed_time = datetime.fromisoformat(
        raw_time.replace("Z", "+00:00")
    )

    if parsed_time.tzinfo is None:
        parsed_time = parsed_time.replace(
            tzinfo=ZoneInfo("America/New_York")
        )

    observed_at = parsed_time.astimezone(
        timezone.utc
    ).isoformat().replace("+00:00", "Z")

    # A unique observation belongs to a road link and time.
    link_id = str(record["link_id"])

    event_identity = (
        f"nyc_dot|{link_id}|{observed_at}"
    )

    event_id = hashlib.sha256(
        event_identity.encode("utf-8")
    ).hexdigest()

    return {
        "event_id": event_id,
        "event_type": "traffic",
        "source": "nyc_dot",
        "observed_at": observed_at,
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "location": {
            "link_id": link_id,
            "borough": record.get("borough"),
            "link_points": record.get("link_points"),
        },
        "measurements": {
            "speed_mph": (
                float(record["speed"])
                if record.get("speed") not in (None, "")
                else None
            ),
            "travel_time_seconds": (
                float(record["travel_time"])
                if record.get("travel_time") not in (None, "")
                else None
            ),
        },
    }

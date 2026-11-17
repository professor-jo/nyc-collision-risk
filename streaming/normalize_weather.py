'''
This script turns a raw Open-Meteo weather response into a clean,
standard weather event that the streaming pipeline can use.

It does three things:
1. Converts the observation time into a full UTC timestamp.
2. Builds a deterministic event_id by hashing the source, location and
   observation time, so the same observation always gets the same ID and
   duplicates can be detected.
3. Renames the measurements to clear names with units (temperature_c,
   precipitation_mm, rain_mm, wind_speed_kmh, weather_code) and adds
   metadata such as the event type, source and ingestion time.
'''

import hashlib
from datetime import datetime, timezone


def normalize_weather(raw_data):
    current = raw_data["current"]

    # The API was requested with timezone="UTC".
    observed_at = current["time"] + ":00Z"

    latitude = raw_data["latitude"]
    longitude = raw_data["longitude"]

    # Same source, location and observation time
    # always produce the same event ID.
    event_identity = (
        f"open_meteo|{latitude}|"
        f"{longitude}|{observed_at}"
    )

    event_id = hashlib.sha256(
        event_identity.encode("utf-8")
    ).hexdigest()

    return {
        "event_id": event_id,
        "event_type": "weather",
        "source": "open_meteo",
        "observed_at": observed_at,
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "location": {
            "latitude": latitude,
            "longitude": longitude
        },
        "measurements": {
            "temperature_c": current["temperature_2m"],
            "precipitation_mm": current["precipitation"],
            "rain_mm": current["rain"],
            "wind_speed_kmh": current["wind_speed_10m"],
            "weather_code": current["weather_code"],
        },
    }

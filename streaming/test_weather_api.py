'''
This script tests the weather side of the streaming pipeline using the
free Open-Meteo API (no API key needed).

1. Fetches the current weather for Manhattan: temperature, precipitation,
   rain, wind speed and the weather code.
2. Prints the raw observation and the units of each value, so we can
   inspect the data structure.
3. Converts the response into a standard event with normalize_weather()
   and prints it.
4. Checks the event's structure with asserts, and that normalizing the
   same observation twice gives the same event_id.

It makes no changes to AWS and incurs no AWS charges.
'''

import json
from urllib.parse import urlencode
from urllib.request import urlopen

from normalize_weather import normalize_weather
from event_contract import validate_event

BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Manhattan, NYC
params = {
    "latitude": 40.7831,
    "longitude": -73.9712,
    "current": (
        "temperature_2m,"
        "precipitation,"
        "rain,"
        "wind_speed_10m,"
        "weather_code"
    ),
    "timezone": "UTC",
}

url = BASE_URL + "?" + urlencode(params)

with urlopen(url, timeout=30) as response:
    data = json.load(response)

print("Weather observation:")
print(json.dumps(data["current"], indent=2))

print("\nMeasurement units:")
print(json.dumps(data["current_units"], indent=2))


# Convert raw weather data into a streaming event
event = normalize_weather(data)

print("\nNormalized weather event:")
print(json.dumps(event, indent=2))

# Validate the standardized event structure
assert event["event_type"] == "weather"
assert event["source"] == "open_meteo"
assert event["event_id"]
assert event["observed_at"].endswith("Z")
assert event["measurements"]["temperature_c"] is not None
assert validate_event(event)


# The same observation should produce the same event ID
event_again = normalize_weather(data)
assert event["event_id"] == event_again["event_id"]

print("\nWeather normalization passed!")
print("Deterministic event ID test passed!")
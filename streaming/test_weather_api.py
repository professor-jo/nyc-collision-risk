'''
This script fetches the current weather for Manhattan from the free
Open-Meteo API (no API key needed) and prints it to the console.

It requests temperature, precipitation, rain, wind speed and the weather
code, then prints the raw JSON observation and the units each value is
measured in, so we can check the data structure before using it.
'''

import json
from urllib.parse import urlencode
from urllib.request import urlopen

from normalize_weather import normalize_weather

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

# The same observation should produce the same event ID
event_again = normalize_weather(data)
assert event["event_id"] == event_again["event_id"]

print("\nWeather normalization passed!")
print("Deterministic event ID test passed!")
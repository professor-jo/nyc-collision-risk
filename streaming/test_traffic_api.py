'''
This script tests the traffic side of the streaming pipeline using NYC
Open Data's real-time traffic speed API (NYC DOT).

1. Requests the five most recent records, ordered by data_as_of.
2. Prints the raw records, so we can inspect the data structure.
3. Converts each record into a standard event with normalize_traffic()
   and prints it.
4. Checks each event's structure with asserts, and that normalizing the
   same record twice gives the same event_id.

It makes no changes to AWS and incurs no AWS charges.
'''
import json
from urllib.parse import urlencode
from urllib.request import urlopen

from normalize_traffic import normalize_traffic
from event_contract import validate_event

BASE_URL = (
    "https://data.cityofnewyork.us/"
    "resource/i4gi-tjb9.json"
)

params = {
    "$select": (
        "link_id,speed,travel_time,"
        "data_as_of,borough,link_points"
    ),
    "$order": "data_as_of DESC",
    "$limit": 5,
}

url = BASE_URL + "?" + urlencode(params)

with urlopen(url, timeout=30) as response:
    records = json.load(response)

print(f"Fetched {len(records)} traffic records")

for record in records:
    print(json.dumps(record, indent=2))


print("\nNormalized traffic events:")

for record in records:
    event = normalize_traffic(record)

    print(json.dumps(event, indent=2))

    assert event["event_type"] == "traffic"
    assert event["source"] == "nyc_dot"
    assert event["event_id"]
    assert event["observed_at"].endswith("Z")
    assert validate_event(event)

    # Same observation = same deterministic ID
    event_again = normalize_traffic(record)

    assert event["event_id"] == event_again["event_id"]

print("\nAll traffic normalization tests passed!")
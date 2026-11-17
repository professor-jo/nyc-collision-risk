'''
1. Connects to NYC open data's traffic speed API
2. Requests five records ordered by the latest data_as_of timestamp
3. Prints records so we can inspect structures
4. Makes no changes to AWS and incurs no AWS charges
'''
import json
from urllib.parse import urlencode
from urllib.request import urlopen

from normalize_traffic import normalize_traffic

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

    # Same observation = same deterministic ID
    event_again = normalize_traffic(record)

    assert event["event_id"] == event_again["event_id"]

print("\nAll traffic normalization tests passed!")
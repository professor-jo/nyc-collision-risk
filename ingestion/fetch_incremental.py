import json
from pathlib import Path


'''
This script processes 500 collision data per run, and it watermarks the last collision_id
so whenever the script is rerun, it will start fetching from the last processed 
collision_id which is represented in the watermark file. 

This way, it fetches only new data 
'''

import requests
# Define the API endpoint for NYC collision data
API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

# Define batch size (number of records to fetch per request)
BATCH_SIZE = 100
# Define the maximum number of records to fetch in total
MAX_RECORDS = 500

# Water mark file to keep track of the last processed crash_id
WATERMARK_FILE = Path("data/state/collision_watermark.json")


# Load the last processed crash_id from the watermark file
def load_watermark():
    if WATERMARK_FILE.exists():
        with open(WATERMARK_FILE, "r") as f:
            return json.load(f)["last_collision_id"]
    # If the watermark file does not exist, return a default value
    return 4900000


# Save the last processed collision_id to the watermark file
def save_watermark(collision_id):
    WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(WATERMARK_FILE, "w") as f:
        json.dump({"last_collision_id": collision_id}, f)


def fetch_incremental():
    # Load and print the last processed collision_id from the watermark file
    last_id = load_watermark()
    print(f"Starting watermark: {last_id}")

    all_records = []

    for offset in range(0, MAX_RECORDS, BATCH_SIZE):
        # Set query parameters this time with collision_id greater than
        # the watermark which is the last processed collision_id
        params = {
            "$where": f"collision_id::number > {last_id}",
            "$order": "collision_id::number ASC",
            "$limit": BATCH_SIZE,
            "$offset": offset
        }

        
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()

        records = response.json()
        all_records.extend(records)

        print(f"Fetched {len(records)} records at offset {offset}")

        if len(records) < BATCH_SIZE:
            break

    # Extract all collision IDs
    collision_ids = [
        int(record["collision_id"])
            for record in all_records
    ]

    # Count total and unique IDs
    total_records = len(collision_ids)
    unique_ids = len(set(collision_ids))

    # Find duplicate IDs
    duplicates = total_records - unique_ids
    print("\n--- Data Validation ---")
    print(f"Total records: {total_records}")
    print(f"Unique collision IDs: {unique_ids}")
    print(f"Duplicate records by ID: {duplicates}")

    # Verify records are ordered numerically
    is_sorted = collision_ids == sorted(collision_ids)
    print(f"Numerically sorted: {is_sorted}")

    # Verify all IDs are greater than the starting watermark
    valid_watermark = all(
        collision_id > int(last_id)
        for collision_id in collision_ids
    )
    print(f"All IDs greater than watermark: {valid_watermark}")

    if collision_ids:
        print(f"Minimum collision ID: {min(collision_ids)}")
        print(f"Maximum collision ID: {max(collision_ids)}")

    
    if all_records:
        new_watermark = max(
            int(record["collision_id"]) for record in all_records
        )

        # save_watermark(new_watermark)
        print(f"Updated watermark to: {new_watermark}")

    print(f"\nTotal records fetched: {len(all_records)}")


if __name__ == "__main__":
    fetch_incremental()
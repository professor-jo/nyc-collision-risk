import requests

'''
This script fetches collision records from the NYC Open Data API
using PAGINATION to retrieve hundreds of records in number of batches.

It fetches 100 records per batch, and continues until it reaches 500
records meaning 5 batches in total.
'''

# Define the API endpoint for NYC collision data
API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"


# Define batch size (number of records to fetch per request) 
BATCH_SIZE = 100
# Define the maximum number of records to fetch in total
MAX_RECORDS = 500


# Collect all records in a list to store the fetched data
all_records = []


# Loop through 5 times (0, 100, 200, 300, 400) to fetch records in batches of 100
for offset in range(0, MAX_RECORDS, BATCH_SIZE):

# Set query parameters to limit the number of records using batch size
# and offset to fetch the next batch of records
# then order them by crash date in descending order
    params = {
        "$limit": BATCH_SIZE,
        "$offset": offset,
        "$order": "crash_date DESC"
    }

# Make a GET request to the API endpoint setting the timeout at 30 seconds
# sending the specified parameters to fetch the next batch of records.
    response = requests.get(
        API_URL,
        params=params,
        timeout=30
    )
 # Check if the request was successful (status code must be 200)
    response.raise_for_status()
    
# Parse the JSON response -> Python data (list of dictionaries)
    records = response.json()

# Append the fetched records
    all_records.extend(records)  
# Append the fetched records to the all_records list
    print(f"Fetched {len(records)} records at offset {offset}")

# Stop early if the API returns fewer records than requested.
    if len(records) < BATCH_SIZE:
        break

print(f"\nTotal records retrieved: {len(all_records)}")
import requests

'''
This script fetches the latest 100 collision records from the NYC Open Data API
then prints the API status code and the first collision record to the console.

It uses the requests library to make an HTTP GET request to the API endpoint.
'''


# Define the API endpoint for NYC collision data
API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"


# Set query parameters to limit the number of records and order 
# them by crash date in descending order
params = {
    "$limit": 100,
    "$order": "crash_date DESC"
}


# Make a GET request to the API endpoint
# with the specified parameters and a timeout at 30 seconds
response = requests.get(
    API_URL,
    params=params,
    timeout=30
)

# Check if the request was successful (status code must be 200)
response.raise_for_status()

# Parse the JSON response -> Python data (list of dictionaries)
records = response.json()

# Print the status code and the number of records retrieved
print(f"Status Code: {response.status_code}")
print(f"Records Retrieved: {len(records)}")


# After retrieving 100 records, print the FIRST record to see the data structure
if records:
    print("First collision record:")
    print(records[0])
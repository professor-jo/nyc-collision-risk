
API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

# Number of records requested per API call.
BATCH_SIZE = 100

# Maximum time spent ingesting during one Lambda execution.
MAX_INGESTION_SECONDS = 240

# Days before the watermark that each run re-fetches for late-arriving records.
LOOKBACK_DAYS = 14

# Most Lambda invocations one Step Functions execution makes to catch up.
MAX_INGESTION_ATTEMPTS = 5

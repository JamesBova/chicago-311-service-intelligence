from datetime import datetime, timedelta

import requests

API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"

# Find the latest modification timestamp in the dataset.
response = requests.get(
    API_URL,
    params={
        "$select": "last_modified_date",
        "$where": "last_modified_date IS NOT NULL",
        "$order": "last_modified_date DESC",
        "$limit": 1
    },
    timeout=30
)

response.raise_for_status()

latest = datetime.fromisoformat(
    response.json()[0]["last_modified_date"]
)

# Establish a 24-hour incremental window.
cutoff = latest - timedelta(hours=24)

params = {
    "$where": f"last_modified_date >= '{cutoff.isoformat()}'",
    "$order": "last_modified_date DESC, sr_number DESC",
    "$limit": 100
}

response = requests.get(API_URL, params=params, timeout=30)
response.raise_for_status()

records = response.json()

print(f"Cutoff timestamp: {cutoff}")
print(f"Records retrieved: {len(records)}")

# Verify every returned record satisfies our filter.
invalid = [
    record
    for record in records
    if datetime.fromisoformat(record["last_modified_date"]) < cutoff
]

print(f"Records outside cutoff: {len(invalid)}")

print("\nSample records:")

for record in records[:5]:
    print(
        record["sr_number"],
        record["created_date"],
        record["last_modified_date"],
        record["status"]
    )
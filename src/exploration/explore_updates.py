import requests
from datetime import datetime, timedelta

API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"

params = {
    "$limit": 1000,
    "$where": "last_modified_date IS NOT NULL",
    "$order": "last_modified_date DESC, sr_number DESC"
}

response = requests.get(API_URL, params=params, timeout=30)
response.raise_for_status()

records = response.json()

print(f"Records retrieved: {len(records)}")

# Identify requests modified more than 24 hours after creation
updated_records = []

for record in records:
    created = datetime.fromisoformat(record["created_date"])
    modified = datetime.fromisoformat(record["last_modified_date"])

    if modified - created > timedelta(hours=24):
        updated_records.append(record)

print(f"Requests modified more than 24 hours after creation: {len(updated_records)}")

print("\nExamples of updated requests:")

for record in updated_records[:5]:
    print(f"\nRequest: {record['sr_number']}")
    print(f"Created: {record['created_date']}")
    print(f"Modified: {record['last_modified_date']}")
    print(f"Status: {record.get('status')}")
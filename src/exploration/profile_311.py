from collections import Counter

import requests

API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"

params = {
    "$limit": 1000,
    "$order": "created_date DESC"
}

response = requests.get(API_URL, params=params, timeout=30)
response.raise_for_status()

records = response.json()

print(f"Records retrieved: {len(records)}")

# Check candidate unique identifiers
request_ids = [record.get("sr_number") for record in records]

print(f"Unique request IDs: {len(set(request_ids))}")
print(f"Missing request IDs: {request_ids.count(None)}")

# Examine request statuses
print("\nService request statuses:")

statuses = Counter(
    record.get("status", "MISSING")
    for record in records
)

for status, count in statuses.most_common():
    print(f"{status}: {count}")

# Identify all fields appearing in the sample
print("\nAvailable fields across all records:")

all_fields = sorted({
    field
    for record in records
    for field in record
})

for field in all_fields:
    print(field)
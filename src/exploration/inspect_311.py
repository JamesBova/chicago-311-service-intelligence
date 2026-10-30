import requests
from pprint import pprint

# Chicago 311 Service Requests API
API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"

# Retrieve five recent service requests
params = {
    "$limit": 5,
    "$order": "created_date DESC"
}

response = requests.get(API_URL, params=params, timeout=30)
response.raise_for_status()

records = response.json()

print(f"Records retrieved: {len(records)}")

if records:
    print("\nAvailable fields:")
    for field in sorted(records[0].keys()):
        print(field)

    print("\nFirst service request:")
    pprint(records[0])
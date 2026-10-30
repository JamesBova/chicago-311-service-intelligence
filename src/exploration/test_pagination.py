import requests
from datetime import datetime, timedelta

API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"


def fetch(params):
    response = requests.get(
        API_URL,
        params=params,
        timeout=30
    )
    response.raise_for_status()
    return response.json()


# Establish a fixed 24-hour window.
latest = fetch({
    "$select": "last_modified_date",
    "$where": "last_modified_date IS NOT NULL",
    "$order": "last_modified_date DESC",
    "$limit": 1
})

end = datetime.fromisoformat(
    latest[0]["last_modified_date"]
)

start = end - timedelta(hours=24)

where_clause = (
    f"last_modified_date >= '{start.isoformat()}' "
    f"AND last_modified_date <= '{end.isoformat()}'"
)

# Count all records matching the window.
result = fetch({
    "$select": "count(*) as total",
    "$where": where_clause
})

total = int(result[0]["total"])

print(f"Records modified in 24 hours: {total}")


# Retrieve two consecutive pages.
def get_page(offset):
    return fetch({
        "$select": "sr_number,last_modified_date",
        "$where": where_clause,
        "$order": "last_modified_date ASC, sr_number ASC",
        "$limit": 100,
        "$offset": offset
    })


page1 = get_page(0)
page2 = get_page(100)

ids1 = {record["sr_number"] for record in page1}
ids2 = {record["sr_number"] for record in page2}

duplicates = ids1.intersection(ids2)

print(f"Page 1 records: {len(page1)}")
print(f"Page 2 records: {len(page2)}")
print(f"Duplicate request IDs between pages: {len(duplicates)}")
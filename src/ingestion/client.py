
from typing import Any

import requests

API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
REQUEST_TIMEOUT = 30


def fetch_records(
    session: requests.Session,
    params: dict[str, str | int],
) -> list[dict[str, Any]]:
    """Retrieve records from the Chicago 311 API."""

    response = session.get(
        API_URL,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()

    records = response.json()

    if not isinstance(records, list):
        raise TypeError("Expected a list of records from the API")

    return records

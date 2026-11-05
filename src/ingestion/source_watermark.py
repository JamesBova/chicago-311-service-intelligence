
from datetime import datetime

import requests

from src.ingestion.client import fetch_records


def get_source_watermark(
    session: requests.Session,
) -> datetime | None:
    """Retrieve the latest available source modification timestamp."""

    params = {
        "$select": "last_modified_date",
        "$where": "last_modified_date IS NOT NULL",
        "$order": "last_modified_date DESC",
        "$limit": 1,
    }

    records = fetch_records(session, params)

    if not records:
        return None

    return datetime.fromisoformat(
        records[0]["last_modified_date"]
    )

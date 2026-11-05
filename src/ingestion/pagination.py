
from collections.abc import Iterator
from typing import Any

import requests

from src.ingestion.client import fetch_records


def fetch_pages_by_cursor(
    session: requests.Session,
    base_params: dict[str, str | int],
    page_size: int = 1000,
) -> Iterator[list[dict[str, Any]]]:
    """Retrieve records using keyset pagination."""

    if page_size <= 0:
        raise ValueError("Page size must be greater than zero")

    original_where = base_params.get("$where")

    if not isinstance(original_where, str) or not original_where.strip():
        raise ValueError("A $where filter is required")

    expected_order = "last_modified_date ASC, sr_number ASC"

    if base_params.get("$order") != expected_order:
        raise ValueError("Unexpected pagination ordering")

    cursor = None

    while True:
        params = {
            **base_params,
            "$limit": page_size,
        }

        if cursor is not None:
            timestamp, request_id = cursor

            timestamp = timestamp.replace("'", "''")
            request_id = request_id.replace("'", "''")

            params["$where"] = (
                f"({original_where}) AND ("
                f"last_modified_date > '{timestamp}' "
                f"OR (last_modified_date = '{timestamp}' "
                f"AND sr_number > '{request_id}'))"
            )

        records = fetch_records(session, params)

        if not records:
            break

        last_record = records[-1]

        new_cursor = (
            last_record["last_modified_date"],
            last_record["sr_number"],
        )

        if cursor is not None and new_cursor <= cursor:
            raise ValueError("Pagination cursor did not advance")

        yield records

        if len(records) < page_size:
            break

        cursor = new_cursor

def fetch_pages(
    session: requests.Session,
    base_params: dict[str, str | int],
    page_size: int = 1000,
) -> Iterator[list[dict[str, Any]]]:
    """Retrieve API records one page at a time."""

    if page_size <= 0:
        raise ValueError("Page size must be greater than zero")

    if not base_params.get("$order"):
        raise ValueError("An explicit $order is required")

    offset = 0

    while True:
        params = {
            **base_params,
            "$limit": page_size,
            "$offset": offset,
        }

        records = fetch_records(session, params)

        if not records:
            break

        yield records

        if len(records) < page_size:
            break

        offset += page_size


import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import requests


API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
REQUEST_TIMEOUT = 30

MAX_ATTEMPTS = 4
RETRY_DELAYS = [15, 30, 60]
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def get_retry_delay(
    response: requests.Response,
    default_delay: int,
) -> float:
    """Use Retry-After when provided; otherwise use backoff."""

    retry_after = response.headers.get("Retry-After")

    if not retry_after:
        return float(default_delay)

    retry_after = retry_after.strip()

    if retry_after.isdecimal():
        return float(retry_after)

    try:
        retry_date = parsedate_to_datetime(retry_after)

        if retry_date.tzinfo is None:
            retry_date = retry_date.replace(tzinfo=timezone.utc)

        return max(
            0.0,
            (retry_date - datetime.now(timezone.utc)).total_seconds(),
        )

    except (ValueError, TypeError, OverflowError):
        return float(default_delay)


def fetch_records(
    session: requests.Session,
    params: dict[str, str | int],
) -> list[dict[str, Any]]:
    """Retrieve Chicago 311 records with automatic retries."""

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = session.get(
                API_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as exc:

            if attempt == MAX_ATTEMPTS:
                logging.error(
                    "Chicago API failed after %s attempts: %s",
                    MAX_ATTEMPTS,
                    exc,
                )
                raise

            delay = RETRY_DELAYS[attempt - 1]

            logging.warning(
                "Chicago API connection failed: %s. "
                "Attempt %s/%s. Retrying in %.0f seconds.",
                exc,
                attempt,
                MAX_ATTEMPTS,
                delay,
            )

            time.sleep(delay)
            continue

        if (
            response.status_code in RETRYABLE_STATUS_CODES
            and attempt < MAX_ATTEMPTS
        ):
            delay = get_retry_delay(
                response,
                RETRY_DELAYS[attempt - 1],
            )

            logging.warning(
                "Chicago API returned HTTP %s. "
                "Attempt %s/%s. Retrying in %.0f seconds.",
                response.status_code,
                attempt,
                MAX_ATTEMPTS,
                delay,
            )

            response.close()
            time.sleep(delay)
            continue

        response.raise_for_status()

        records = response.json()

        if not isinstance(records, list):
            raise TypeError(
                "Expected a list of records from the API"
            )

        return records

    raise RuntimeError("Chicago API retries exhausted")

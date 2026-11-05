
from unittest.mock import Mock

import pytest
import requests

from src.ingestion.client import API_URL, REQUEST_TIMEOUT, fetch_records


def test_fetch_records_success():
    # Simulate a successful API response.
    session = Mock(spec=requests.Session)
    response = Mock(spec=requests.Response)

    expected_records = [
        {"sr_number": "SR001", "status": "Open"},
        {"sr_number": "SR002", "status": "Completed"},
    ]

    response.json.return_value = expected_records
    session.get.return_value = response

    params = {"$limit": 2}

    records = fetch_records(session, params)

    assert records == expected_records

    session.get.assert_called_once_with(
        API_URL,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status.assert_called_once()


def test_fetch_records_http_error():
    # Simulate an HTTP error from the API.
    session = Mock(spec=requests.Session)
    response = Mock(spec=requests.Response)

    response.raise_for_status.side_effect = requests.HTTPError(
        "500 Server Error"
    )
    session.get.return_value = response

    with pytest.raises(requests.HTTPError):
        fetch_records(session, {"$limit": 2})


def test_fetch_records_invalid_response():
    # Simulate an unexpected JSON response type.
    session = Mock(spec=requests.Session)
    response = Mock(spec=requests.Response)

    response.json.return_value = {"error": "Unexpected response"}
    session.get.return_value = response

    with pytest.raises(TypeError):
        fetch_records(session, {"$limit": 2})

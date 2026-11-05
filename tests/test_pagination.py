
from unittest.mock import Mock, call, patch

import pytest

from src.ingestion.pagination import fetch_pages, fetch_pages_by_cursor


def test_fetch_pages_multiple_pages():
    session = Mock()

    # Simulate three API responses.
    mock_pages = [
        [{"sr_number": "SR001"}, {"sr_number": "SR002"}],
        [{"sr_number": "SR003"}, {"sr_number": "SR004"}],
        [{"sr_number": "SR005"}],
    ]

    params = {"$order": "sr_number ASC"}

    with patch(
        "src.ingestion.pagination.fetch_records"
    ) as mock_fetch:
        mock_fetch.side_effect = mock_pages

        pages = list(fetch_pages(session, params, page_size=2))

        assert len(pages) == 3
        assert sum(len(page) for page in pages) == 5

        assert mock_fetch.call_args_list == [
            call(session, {
                "$order": "sr_number ASC",
                "$limit": 2,
                "$offset": 0,
            }),
            call(session, {
                "$order": "sr_number ASC",
                "$limit": 2,
                "$offset": 2,
            }),
            call(session, {
                "$order": "sr_number ASC",
                "$limit": 2,
                "$offset": 4,
            }),
        ]


def test_fetch_pages_empty_response():
    session = Mock()

    with patch(
        "src.ingestion.pagination.fetch_records",
        return_value=[],
    ) as mock_fetch:
        pages = list(
            fetch_pages(
                session,
                {"$order": "sr_number ASC"},
                page_size=2,
            )
        )

        assert pages == []
        assert mock_fetch.call_count == 1


def test_fetch_pages_invalid_page_size():
    with pytest.raises(ValueError):
        list(
            fetch_pages(
                Mock(),
                {"$order": "sr_number ASC"},
                page_size=0,
            )
        )


def test_fetch_pages_requires_order():
    with pytest.raises(ValueError):
        list(
            fetch_pages(
                Mock(),
                {},
                page_size=100,
            )
        )

def test_cursor_pagination_multiple_pages():
    session = Mock()

    # Multiple records can share the same timestamp.
    mock_pages = [
        [
            {
                "sr_number": "SR001",
                "last_modified_date": "2026-10-29T08:00:00",
            },
            {
                "sr_number": "SR002",
                "last_modified_date": "2026-10-29T08:00:00",
            },
        ],
        [
            {
                "sr_number": "SR003",
                "last_modified_date": "2026-10-29T08:00:00",
            },
        ],
    ]

    params = {
        "$where": "last_modified_date >= '2026-10-29T08:00:00'",
        "$order": "last_modified_date ASC, sr_number ASC",
    }

    with patch(
        "src.ingestion.pagination.fetch_records",
        side_effect=mock_pages,
    ) as mock_fetch:
        pages = list(
            fetch_pages_by_cursor(session, params, page_size=2)
        )

    assert len(pages) == 2
    assert sum(len(page) for page in pages) == 3

    # First request uses the original filter.
    first_params = mock_fetch.call_args_list[0].args[1]
    assert first_params["$where"] == params["$where"]
    assert first_params["$limit"] == 2

    # Second request continues after SR002.
    second_params = mock_fetch.call_args_list[1].args[1]

    assert "sr_number > 'SR002'" in second_params["$where"]
    assert "last_modified_date =" in second_params["$where"]

    # Cursor pagination should not use offsets.
    assert "$offset" not in first_params
    assert "$offset" not in second_params


def test_cursor_pagination_empty_response():
    session = Mock()

    params = {
        "$where": "last_modified_date IS NOT NULL",
        "$order": "last_modified_date ASC, sr_number ASC",
    }

    with patch(
        "src.ingestion.pagination.fetch_records",
        return_value=[],
    ) as mock_fetch:
        pages = list(fetch_pages_by_cursor(session, params))

    assert pages == []
    assert mock_fetch.call_count == 1


def test_cursor_pagination_repeated_cursor():
    session = Mock()

    record = {
        "sr_number": "SR001",
        "last_modified_date": "2026-10-29T08:00:00",
    }

    params = {
        "$where": "last_modified_date IS NOT NULL",
        "$order": "last_modified_date ASC, sr_number ASC",
    }

    # Simulate an API that keeps returning the same record.
    with (
        patch(
            "src.ingestion.pagination.fetch_records",
            side_effect=[[record], [record]],
        ),
        pytest.raises(ValueError, match="cursor did not advance"),
    ):
        list(fetch_pages_by_cursor(session, params, page_size=1))


def test_cursor_pagination_invalid_page_size():
    session = Mock()

    params = {
        "$where": "last_modified_date IS NOT NULL",
        "$order": "last_modified_date ASC, sr_number ASC",
    }

    with pytest.raises(ValueError):
        list(fetch_pages_by_cursor(session, params, page_size=0))


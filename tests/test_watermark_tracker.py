from datetime import datetime

import pytest

from src.ingestion.watermark_tracker import (
    WatermarkTracker,
    track_pages,
)


def test_tracks_maximum_timestamp():

    pages = [
        [
            {
                "sr_number": "SR100",
                "last_modified_date": "2026-10-31T08:15:00",
            },
            {
                "sr_number": "SR101",
                "last_modified_date": "2026-10-31T08:32:00",
            },
        ],
        [
            {
                "sr_number": "SR102",
                "last_modified_date": "2026-10-31T08:21:00",
            },
        ],
    ]

    tracker = WatermarkTracker()

    result = list(track_pages(pages, tracker))

    assert result == pages

    assert tracker.latest == datetime(
        2026, 10, 31, 8, 32
    )


def test_empty_extraction():

    tracker = WatermarkTracker()

    result = list(track_pages([], tracker))

    assert result == []
    assert tracker.latest is None


def test_missing_timestamp():

    tracker = WatermarkTracker()

    pages = [[{"sr_number": "SR100"}]]

    with pytest.raises(ValueError):
        list(track_pages(pages, tracker))
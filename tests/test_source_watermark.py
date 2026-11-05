
from datetime import datetime
from unittest.mock import Mock, patch

from src.ingestion.source_watermark import get_source_watermark


def test_source_watermark_success():
    session = Mock()

    with patch(
        "src.ingestion.source_watermark.fetch_records",
        return_value=[
            {"last_modified_date": "2026-10-31T08:14:51.000"}
        ],
    ):
        result = get_source_watermark(session)

    assert result == datetime(2026, 10, 31, 8, 14, 51)


def test_source_watermark_empty():
    session = Mock()

    with patch(
        "src.ingestion.source_watermark.fetch_records",
        return_value=[],
    ):
        result = get_source_watermark(session)

    assert result is None


from datetime import UTC, datetime

import pytest

from src.ingestion.source_time import to_source_time


def test_daylight_saving_time():
    instant = datetime(2026, 10, 31, 13, 0, tzinfo=UTC)

    result = to_source_time(instant)

    assert result == datetime(2026, 10, 31, 8, 0)


def test_standard_time():
    instant = datetime(2026, 12, 1, 14, 0, tzinfo=UTC)

    result = to_source_time(instant)

    assert result == datetime(2026, 12, 1, 8, 0)


def test_reject_naive_datetime():
    instant = datetime(2026, 10, 31, 13, 0)

    with pytest.raises(ValueError):
        to_source_time(instant)

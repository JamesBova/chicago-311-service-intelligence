
from datetime import datetime

import pytest

from src.ingestion.incremental import build_incremental_params


def test_build_incremental_params():
    start = datetime(2026, 10, 29, 8, 0, 0)
    end = datetime(2026, 10, 29, 9, 0, 0)

    params = build_incremental_params(start, end)

    assert params["$where"] == (
        "last_modified_date >= '2026-10-29T08:00:00' "
        "AND last_modified_date < '2026-10-29T09:00:00'"
    )

    assert params["$order"] == (
        "last_modified_date ASC, sr_number ASC"
    )


def test_incremental_params_equal_timestamps():
    timestamp = datetime(2026, 10, 29, 8, 0, 0)

    with pytest.raises(ValueError):
        build_incremental_params(timestamp, timestamp)


def test_incremental_params_reversed_timestamps():
    start = datetime(2026, 10, 29, 9, 0, 0)
    end = datetime(2026, 10, 29, 8, 0, 0)

    with pytest.raises(ValueError):
        build_incremental_params(start, end)

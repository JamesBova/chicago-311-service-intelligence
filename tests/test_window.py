
from datetime import datetime, timedelta

import pytest

from src.ingestion.window import calculate_window


def test_first_run():
    result = calculate_window(
        current_time=datetime(2026, 10, 30, 10, 0),
        initial_start=datetime(2026, 10, 30, 8, 0),
        checkpoint=None,
    )

    assert result == (
        datetime(2026, 10, 30, 8, 0),
        datetime(2026, 10, 30, 9, 50),
    )


def test_resume_with_overlap():
    result = calculate_window(
        current_time=datetime(2026, 10, 30, 10, 0),
        initial_start=datetime(2026, 10, 28, 8, 0),
        checkpoint=datetime(2026, 10, 30, 9, 0),
    )

    assert result == (
        datetime(2026, 10, 29, 9, 0),
        datetime(2026, 10, 30, 9, 50),
    )


def test_overlap_respects_initial_start():
    result = calculate_window(
        current_time=datetime(2026, 10, 30, 10, 0),
        initial_start=datetime(2026, 10, 30, 8, 0),
        checkpoint=datetime(2026, 10, 30, 9, 0),
    )

    assert result == (
        datetime(2026, 10, 30, 8, 0),
        datetime(2026, 10, 30, 9, 50),
    )


def test_no_new_window():
    result = calculate_window(
        current_time=datetime(2026, 10, 30, 9, 5),
        initial_start=datetime(2026, 10, 30, 8, 0),
        checkpoint=datetime(2026, 10, 30, 9, 0),
    )

    assert result is None


def test_first_run_too_early():
    result = calculate_window(
        current_time=datetime(2026, 10, 30, 8, 5),
        initial_start=datetime(2026, 10, 30, 8, 0),
        checkpoint=None,
    )

    assert result is None


@pytest.mark.parametrize(
    ("safety_delay", "replay_overlap"),
    [
        (timedelta(minutes=-1), timedelta(hours=24)),
        (timedelta(minutes=10), timedelta(hours=-1)),
    ],
)
def test_negative_configuration(safety_delay, replay_overlap):
    with pytest.raises(ValueError):
        calculate_window(
            current_time=datetime(2026, 10, 30, 10, 0),
            initial_start=datetime(2026, 10, 30, 8, 0),
            checkpoint=None,
            safety_delay=safety_delay,
            replay_overlap=replay_overlap,
        )


def test_daylight_saving_fallback():
    initial_start = datetime(2026, 10, 30)
    checkpoint = datetime(2026, 11, 1, 1, 55)

    # Clock moved backward during the repeated 1 AM hour.
    result = calculate_window(
        current_time=datetime(2026, 11, 1, 1, 20),
        initial_start=initial_start,
        checkpoint=checkpoint,
    )

    assert result is None

    # After the repeated hour, the overlap permits replay.
    result = calculate_window(
        current_time=datetime(2026, 11, 1, 2, 20),
        initial_start=initial_start,
        checkpoint=checkpoint,
    )

    assert result == (
        datetime(2026, 10, 31, 1, 55),
        datetime(2026, 11, 1, 2, 10),
    )

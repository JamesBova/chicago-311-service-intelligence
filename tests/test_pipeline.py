
import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from src.ingestion.checkpoint import load_checkpoint, save_checkpoint
from src.ingestion.pipeline import run_ingestion


def test_pipeline_success(tmp_path):
    session = Mock()
    checkpoint_path = tmp_path / "state" / "checkpoint.json"
    raw_directory = tmp_path / "raw"

    pages = iter([
        [
            {
                "sr_number": "SR001",
                "last_modified_date": "2026-10-29T08:15:00",
            }
        ],
        [
            {
                "sr_number": "SR002",
                "last_modified_date": "2026-10-29T08:45:00",
            }
        ],
    ])

    with patch(
        "src.ingestion.pipeline.fetch_pages_by_cursor",
        return_value=pages,
    ) as mock_fetch:
        result = run_ingestion(
            session=session,
            checkpoint_path=checkpoint_path,
            raw_directory=raw_directory,
            initial_start=datetime(2026, 10, 29, 8, 0),
            current_time=datetime(2026, 10, 29, 9, 10),
            page_size=2,
        )

    assert result is not None
    count, destination = result

    assert count == 2
    assert destination.exists()

    records = [
        json.loads(line)
        for line in destination.read_text().splitlines()
    ]

    assert len(records) == 2
    assert records[0]["sr_number"] == "SR001"
    assert records[1]["sr_number"] == "SR002"

    assert load_checkpoint(checkpoint_path) == datetime(
        2026, 10, 29, 8, 45
    )

    mock_fetch.assert_called_once()


def test_pipeline_failure_preserves_checkpoint(tmp_path):
    session = Mock()
    checkpoint_path = tmp_path / "state" / "checkpoint.json"
    raw_directory = tmp_path / "raw"

    previous_watermark = datetime(2026, 10, 29, 9, 0)

    save_checkpoint(checkpoint_path, previous_watermark)

    def failing_pages():
        yield [
            {
                "sr_number": "SR001",
                "last_modified_date": "2026-10-29T09:15:00",
            }
        ]

        raise RuntimeError("Simulated API failure")

    with (
        patch(
            "src.ingestion.pipeline.fetch_pages_by_cursor",
            return_value=failing_pages(),
        ),
        pytest.raises(RuntimeError, match="Simulated API failure"),
    ):
        run_ingestion(
            session=session,
            checkpoint_path=checkpoint_path,
            raw_directory=raw_directory,
            initial_start=datetime(2026, 10, 28, 8, 0),
            current_time=datetime(2026, 10, 29, 10, 10),
        )

    assert load_checkpoint(checkpoint_path) == previous_watermark


def test_pipeline_resumes_from_checkpoint(tmp_path):
    session = Mock()
    checkpoint_path = tmp_path / "checkpoint.json"
    raw_directory = tmp_path / "raw"

    previous_watermark = datetime(2026, 10, 29, 9, 0)

    save_checkpoint(checkpoint_path, previous_watermark)

    with patch(
        "src.ingestion.pipeline.fetch_pages_by_cursor",
        return_value=iter([]),
    ) as mock_fetch:
        run_ingestion(
            session=session,
            checkpoint_path=checkpoint_path,
            raw_directory=raw_directory,
            initial_start=datetime(2026, 10, 28, 8, 0),
            current_time=datetime(2026, 10, 29, 10, 10),
        )

    params = mock_fetch.call_args.args[1]

    # Watermark minus 24 hours.
    assert "2026-10-28T09:00:00" in params["$where"]

    # No records means no watermark advancement.
    assert load_checkpoint(checkpoint_path) == previous_watermark


def test_pipeline_skips_when_no_new_window(tmp_path):
    session = Mock()
    checkpoint_path = tmp_path / "checkpoint.json"
    raw_directory = tmp_path / "raw"

    watermark = datetime(2026, 10, 30, 9, 0)

    save_checkpoint(checkpoint_path, watermark)

    with patch(
        "src.ingestion.pipeline.fetch_pages_by_cursor",
    ) as mock_fetch:
        result = run_ingestion(
            session=session,
            checkpoint_path=checkpoint_path,
            raw_directory=raw_directory,
            initial_start=datetime(2026, 10, 30, 8, 0),
            current_time=datetime(2026, 10, 30, 9, 5),
        )

    assert result is None
    mock_fetch.assert_not_called()
    assert load_checkpoint(checkpoint_path) == watermark


def test_zero_records_does_not_advance_watermark(tmp_path):
    session = Mock()
    checkpoint_path = tmp_path / "checkpoint.json"
    raw_directory = tmp_path / "raw"

    with patch(
        "src.ingestion.pipeline.fetch_pages_by_cursor",
        return_value=iter([]),
    ):
        result = run_ingestion(
            session=session,
            checkpoint_path=checkpoint_path,
            raw_directory=raw_directory,
            initial_start=datetime(2026, 10, 29, 8, 0),
            current_time=datetime(2026, 10, 29, 10, 10),
        )

    assert result is not None
    count, destination = result

    assert count == 0
    assert destination.exists()
    assert load_checkpoint(checkpoint_path) is None


def test_overlap_does_not_move_watermark_backward(tmp_path):
    session = Mock()
    checkpoint_path = tmp_path / "checkpoint.json"
    raw_directory = tmp_path / "raw"

    previous_watermark = datetime(2026, 10, 29, 9, 0)

    save_checkpoint(checkpoint_path, previous_watermark)

    pages = iter([
        [
            {
                "sr_number": "SR001",
                "last_modified_date": "2026-10-29T08:30:00",
            }
        ]
    ])

    with patch(
        "src.ingestion.pipeline.fetch_pages_by_cursor",
        return_value=pages,
    ):
        run_ingestion(
            session=session,
            checkpoint_path=checkpoint_path,
            raw_directory=raw_directory,
            initial_start=datetime(2026, 10, 28, 8, 0),
            current_time=datetime(2026, 10, 29, 10, 10),
        )

    assert load_checkpoint(checkpoint_path) == previous_watermark


def test_newer_record_advances_watermark(tmp_path):
    session = Mock()
    checkpoint_path = tmp_path / "checkpoint.json"
    raw_directory = tmp_path / "raw"

    previous_watermark = datetime(2026, 10, 29, 9, 0)

    save_checkpoint(checkpoint_path, previous_watermark)

    pages = iter([
        [
            {
                "sr_number": "SR001",
                "last_modified_date": "2026-10-29T08:30:00",
            },
            {
                "sr_number": "SR002",
                "last_modified_date": "2026-10-29T09:30:00",
            },
        ]
    ])

    with patch(
        "src.ingestion.pipeline.fetch_pages_by_cursor",
        return_value=pages,
    ):
        run_ingestion(
            session=session,
            checkpoint_path=checkpoint_path,
            raw_directory=raw_directory,
            initial_start=datetime(2026, 10, 28, 8, 0),
            current_time=datetime(2026, 10, 29, 10, 10),
        )

    assert load_checkpoint(checkpoint_path) == datetime(
        2026, 10, 29, 9, 30
    )


def test_missing_timestamp_preserves_checkpoint(tmp_path):
    session = Mock()
    checkpoint_path = tmp_path / "checkpoint.json"
    raw_directory = tmp_path / "raw"

    previous_watermark = datetime(2026, 10, 29, 9, 0)

    save_checkpoint(checkpoint_path, previous_watermark)

    pages = iter([
        [{"sr_number": "SR001"}]
    ])

    with (
        patch(
            "src.ingestion.pipeline.fetch_pages_by_cursor",
            return_value=pages,
        ),
        pytest.raises(
            ValueError,
            match="missing last_modified_date",
        ),
    ):
        run_ingestion(
            session=session,
            checkpoint_path=checkpoint_path,
            raw_directory=raw_directory,
            initial_start=datetime(2026, 10, 28, 8, 0),
            current_time=datetime(2026, 10, 29, 10, 10),
        )

    assert load_checkpoint(checkpoint_path) == previous_watermark

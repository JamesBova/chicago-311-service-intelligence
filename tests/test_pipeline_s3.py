
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from src.ingestion.checkpoint import load_checkpoint, save_checkpoint
from src.ingestion.pipeline import run_ingestion


def test_failed_s3_upload_preserves_checkpoint(tmp_path):
    checkpoint_path = tmp_path / "checkpoint.json"
    raw_directory = tmp_path / "raw"

    # Simulate a previously successful ingestion.
    previous_checkpoint = datetime(2026, 10, 31, 8, 0)
    save_checkpoint(checkpoint_path, previous_checkpoint)

    # Simulate a new record retrieved from Chicago 311.
    pages = iter(
        [
            [
                {
                    "sr_number": "SR-TEST-001",
                    "last_modified_date": "2026-10-31T09:00:00.000",
                }
            ]
        ]
    )

    # Simulate an AWS S3 upload failure.
    uploader = Mock(
        side_effect=RuntimeError("Simulated S3 failure")
    )

    with patch(
        "src.ingestion.pipeline.fetch_pages_by_cursor",
        return_value=pages,
    ), pytest.raises(
        RuntimeError,
        match="Simulated S3 failure",
    ):
        run_ingestion(
            session=Mock(),
            checkpoint_path=checkpoint_path,
            raw_directory=raw_directory,
            initial_start=datetime(2026, 10, 31, 7, 0),
            current_time=datetime(2026, 10, 31, 10, 10),
            upload_file=uploader,
        )

    # Confirm an upload was attempted.
    uploader.assert_called_once()

    # The failed upload must not advance the checkpoint.
    assert load_checkpoint(checkpoint_path) == previous_checkpoint

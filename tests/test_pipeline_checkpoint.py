
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from src.ingestion.pipeline import run_ingestion


def make_test_pages():
    return iter(
        [
            [
                {
                    "sr_number": "SR-TEST-001",
                    "last_modified_date": "2026-10-31T09:00:00.000",
                }
            ]
        ]
    )


def test_custom_checkpoint_advances_after_upload(tmp_path):
    previous_checkpoint = datetime(2026, 10, 31, 8, 0)
    new_checkpoint = datetime(2026, 10, 31, 9, 0)

    load_state = Mock(return_value=previous_checkpoint)
    save_state = Mock()
    upload_file = Mock()

    with patch(
        "src.ingestion.pipeline.fetch_pages_by_cursor",
        return_value=make_test_pages(),
    ):
        run_ingestion(
            session=Mock(),
            checkpoint_path=tmp_path / "checkpoint.json",
            raw_directory=tmp_path / "raw",
            initial_start=datetime(2026, 10, 31, 7, 0),
            current_time=datetime(2026, 10, 31, 10, 10),
            upload_file=upload_file,
            load_state=load_state,
            save_state=save_state,
        )

    load_state.assert_called_once()
    upload_file.assert_called_once()
    save_state.assert_called_once_with(new_checkpoint)

    # Custom state functions should not create a local checkpoint.
    assert not (tmp_path / "checkpoint.json").exists()


def test_failed_upload_does_not_save_custom_checkpoint(tmp_path):
    load_state = Mock(
        return_value=datetime(2026, 10, 31, 8, 0)
    )
    save_state = Mock()
    upload_file = Mock(
        side_effect=RuntimeError("S3 upload failed")
    )

    with (
        patch(
            "src.ingestion.pipeline.fetch_pages_by_cursor",
            return_value=make_test_pages(),
        ),
        pytest.raises(RuntimeError, match="S3 upload failed"),
    ):
        run_ingestion(
            session=Mock(),
            checkpoint_path=tmp_path / "checkpoint.json",
            raw_directory=tmp_path / "raw",
            initial_start=datetime(2026, 10, 31, 7, 0),
            current_time=datetime(2026, 10, 31, 10, 10),
            upload_file=upload_file,
            load_state=load_state,
            save_state=save_state,
        )

    save_state.assert_not_called()


def test_failed_checkpoint_write_propagates(tmp_path):
    load_state = Mock(
        return_value=datetime(2026, 10, 31, 8, 0)
    )
    save_state = Mock(
        side_effect=RuntimeError("S3 checkpoint write failed")
    )
    upload_file = Mock()

    with (
        patch(
            "src.ingestion.pipeline.fetch_pages_by_cursor",
            return_value=make_test_pages(),
        ),
        pytest.raises(
            RuntimeError,
            match="S3 checkpoint write failed",
        ),
    ):
        run_ingestion(
            session=Mock(),
            checkpoint_path=tmp_path / "checkpoint.json",
            raw_directory=tmp_path / "raw",
            initial_start=datetime(2026, 10, 31, 7, 0),
            current_time=datetime(2026, 10, 31, 10, 10),
            upload_file=upload_file,
            load_state=load_state,
            save_state=save_state,
        )

    upload_file.assert_called_once()
    save_state.assert_called_once()

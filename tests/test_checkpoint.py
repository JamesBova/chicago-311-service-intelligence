
import json
from datetime import datetime

import pytest

from src.ingestion.checkpoint import load_checkpoint, save_checkpoint


def test_load_checkpoint_missing_file(tmp_path):
    # No checkpoint exists on the first run.
    checkpoint_path = tmp_path / "checkpoint.json"

    result = load_checkpoint(checkpoint_path)

    assert result is None


def test_save_and_load_checkpoint(tmp_path):
    # Save a checkpoint and verify it can be retrieved.
    checkpoint_path = tmp_path / "state" / "checkpoint.json"
    timestamp = datetime(2026, 10, 30, 9, 0, 0)

    save_checkpoint(checkpoint_path, timestamp)

    assert checkpoint_path.exists()
    assert load_checkpoint(checkpoint_path) == timestamp

    # The temporary file should no longer exist.
    assert not checkpoint_path.with_suffix(".json.tmp").exists()


def test_checkpoint_updates_after_success(tmp_path):
    # Simulate two successful ingestion runs.
    checkpoint_path = tmp_path / "checkpoint.json"

    first_run = datetime(2026, 10, 30, 9, 0, 0)
    second_run = datetime(2026, 10, 30, 10, 0, 0)

    save_checkpoint(checkpoint_path, first_run)
    save_checkpoint(checkpoint_path, second_run)

    assert load_checkpoint(checkpoint_path) == second_run


def test_checkpoint_invalid_json(tmp_path):
    # Corrupted checkpoints should raise an error.
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text("{invalid json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        load_checkpoint(checkpoint_path)

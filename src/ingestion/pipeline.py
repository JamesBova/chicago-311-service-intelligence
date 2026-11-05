from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import requests

from src.ingestion.checkpoint import load_checkpoint, save_checkpoint
from src.ingestion.incremental import build_incremental_params
from src.ingestion.pagination import fetch_pages_by_cursor
from src.ingestion.storage import write_raw_pages
from src.ingestion.watermark_tracker import WatermarkTracker, track_pages
from src.ingestion.window import calculate_window


def run_ingestion(
    session: requests.Session,
    checkpoint_path: Path,
    raw_directory: Path,
    initial_start: datetime,
    current_time: datetime,
    page_size: int = 1000,
    upload_file: Callable[[Path], str] | None = None,
    load_state: Callable[[], datetime | None] | None = None,
    save_state: Callable[[datetime], None] | None = None,
    save_manifest: Callable[[str, int], None] | None = None,
) -> tuple[int, Path] | None:
    """Run incremental Chicago 311 ingestion."""

    # Use local checkpoint storage unless custom functions are provided.
    if load_state is None:
        load_state = lambda: load_checkpoint(checkpoint_path)

    if save_state is None:
        save_state = lambda value: save_checkpoint(
            checkpoint_path,
            value,
        )

    checkpoint = load_state()

    window = calculate_window(
        current_time=current_time,
        initial_start=initial_start,
        checkpoint=checkpoint,
    )

    if window is None:
        return None

    start, end = window

    params = build_incremental_params(start, end)

    filename = (
        f"chicago311_{start:%Y%m%dT%H%M%S%f}_"
        f"{end:%Y%m%dT%H%M%S%f}.jsonl"
    )

    destination = raw_directory / filename

    pages = fetch_pages_by_cursor(
        session,
        params,
        page_size=page_size,
    )

    tracker = WatermarkTracker()
    pages = track_pages(pages, tracker)

    record_count = write_raw_pages(pages, destination)

    # A failed raw upload or manifest upload prevents checkpoint advancement.
    if record_count > 0 and upload_file is not None:
        raw_file_uri = upload_file(destination)

        if save_manifest is not None:
            save_manifest(raw_file_uri, record_count)

    # A failed checkpoint write propagates as an error.
    if tracker.latest is not None and (
        checkpoint is None or tracker.latest > checkpoint
    ):
        save_state(tracker.latest)

    return record_count, destination
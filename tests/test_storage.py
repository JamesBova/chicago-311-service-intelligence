
import json

import pytest

from src.ingestion.storage import write_raw_pages


def test_write_raw_pages_success(tmp_path):
    destination = tmp_path / "raw" / "batch.jsonl"

    pages = [
        [
            {"sr_number": "SR001", "status": "Open"},
            {"sr_number": "SR002", "status": "Completed"},
        ],
        [
            {"sr_number": "SR003", "status": "Open"},
        ],
    ]

    count = write_raw_pages(pages, destination)

    assert count == 3
    assert destination.exists()

    lines = destination.read_text(
        encoding="utf-8"
    ).splitlines()

    records = [json.loads(line) for line in lines]

    assert records == [
        record for page in pages for record in page
    ]


def test_write_raw_pages_empty(tmp_path):
    destination = tmp_path / "batch.jsonl"

    count = write_raw_pages([], destination)

    assert count == 0
    assert destination.exists()
    assert destination.read_text(encoding="utf-8") == ""


def test_write_raw_pages_failure(tmp_path):
    destination = tmp_path / "batch.jsonl"

    # Simulate an existing successful extraction.
    original_content = '{"sr_number": "OLD"}\n'
    destination.write_text(
        original_content,
        encoding="utf-8",
    )

    # Simulate an API failure after the first page.
    def failing_pages():
        yield [{"sr_number": "SR001"}]
        raise RuntimeError("API request failed")

    with pytest.raises(RuntimeError):
        write_raw_pages(failing_pages(), destination)

    # Previously completed data must remain intact.
    assert destination.read_text(
        encoding="utf-8"
    ) == original_content

    # Temporary files must be cleaned up.
    assert list(tmp_path.glob(".batch.jsonl.*.tmp")) == []


def test_write_raw_pages_invalid_record(tmp_path):
    destination = tmp_path / "batch.jsonl"

    # An object that cannot be serialized to JSON.
    pages = [[{"invalid": object()}]]

    with pytest.raises(TypeError):
        write_raw_pages(pages, destination)

    assert not destination.exists()
    assert list(tmp_path.glob(".batch.jsonl.*.tmp")) == []

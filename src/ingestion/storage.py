
import json
from collections.abc import Iterable
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


def write_raw_pages(
    pages: Iterable[list[dict[str, Any]]],
    destination: Path,
) -> int:
    """Write API pages to a JSONL file safely."""

    destination.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = None
    record_count = 0

    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)

            for page in pages:
                for record in page:
                    temporary_file.write(
                        json.dumps(record, ensure_ascii=False) + "\n"
                    )
                    record_count += 1

        temporary_path.replace(destination)

        return record_count

    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise

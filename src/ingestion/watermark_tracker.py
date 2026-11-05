from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class WatermarkTracker:
    latest: datetime | None = None

    def observe(self, record: dict[str, Any]) -> None:
        raw_timestamp = record.get("last_modified_date")

        if not raw_timestamp:
            raise ValueError(
                "Record is missing last_modified_date"
            )

        timestamp = datetime.fromisoformat(raw_timestamp)

        if timestamp.tzinfo is not None:
            raise ValueError(
                "Expected a timezone-naive source timestamp"
            )

        if self.latest is None or timestamp > self.latest:
            self.latest = timestamp


def track_pages(
    pages: Iterable[list[dict[str, Any]]],
    tracker: WatermarkTracker,
) -> Iterator[list[dict[str, Any]]]:

    for page in pages:
        for record in page:
            tracker.observe(record)

        yield page
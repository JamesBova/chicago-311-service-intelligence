
import json
from datetime import datetime
from pathlib import Path


def load_checkpoint(path: Path) -> datetime | None:
    """Read the last successfully completed extraction boundary."""

    if not path.exists():
        return None

    state = json.loads(path.read_text(encoding="utf-8"))

    return datetime.fromisoformat(
        state["last_successful_end"]
    )


def save_checkpoint(path: Path, end: datetime) -> None:
    """Save the completed extraction boundary."""

    path.parent.mkdir(parents=True, exist_ok=True)

    state = {
        "last_successful_end": end.isoformat()
    }

    temporary_path = path.with_suffix(".json.tmp")

    temporary_path.write_text(
        json.dumps(state, indent=2),
        encoding="utf-8",
    )

    temporary_path.replace(path)

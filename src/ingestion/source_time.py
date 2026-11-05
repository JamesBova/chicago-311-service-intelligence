
from datetime import datetime
from zoneinfo import ZoneInfo

CHICAGO_TIMEZONE = ZoneInfo("America/Chicago")


def to_source_time(instant: datetime) -> datetime:
    """Convert an aware timestamp to Chicago local source time."""

    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("An offset-aware datetime is required")

    return instant.astimezone(
        CHICAGO_TIMEZONE
    ).replace(tzinfo=None)

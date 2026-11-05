
from datetime import datetime, timedelta


def calculate_window(
    current_time: datetime,
    initial_start: datetime,
    checkpoint: datetime | None,
    safety_delay: timedelta = timedelta(minutes=10),
    replay_overlap: timedelta = timedelta(hours=24),
) -> tuple[datetime, datetime] | None:
    """Calculate the incremental extraction window."""

    if safety_delay < timedelta(0):
        raise ValueError("Safety delay cannot be negative")

    if replay_overlap < timedelta(0):
        raise ValueError("Replay overlap cannot be negative")

    # Keep the extraction endpoint slightly behind current time.
    end = current_time - safety_delay

    # Do not run if the endpoint has not passed the watermark.
    if checkpoint is not None and end <= checkpoint:
        return None

    # Reprocess previous records to accommodate late arrivals.
    if checkpoint is None:
        start = initial_start
    else:
        start = max(
            initial_start,
            checkpoint - replay_overlap,
        )

    # No valid extraction window exists.
    if start >= end:
        return None

    return start, end

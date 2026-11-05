
from datetime import datetime


def build_incremental_params(
    start: datetime,
    end: datetime,
) -> dict[str, str]:
    """Build API parameters for an incremental extraction window."""

    if start >= end:
        raise ValueError("Start must be earlier than end")

    where_clause = (
        f"last_modified_date >= '{start.isoformat()}' "
        f"AND last_modified_date < '{end.isoformat()}'"
    )

    return {
        "$where": where_clause,
        "$order": "last_modified_date ASC, sr_number ASC",
    }

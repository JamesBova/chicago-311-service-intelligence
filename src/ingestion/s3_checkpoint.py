
"""Persistent ingestion checkpoints stored in Amazon S3."""

import json
from datetime import datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError


def load_s3_checkpoint(
    bucket: str,
    key: str = "dev/state/checkpoint.json",
    s3_client: Any | None = None,
) -> datetime | None:
    """Load the last successfully processed timestamp from S3."""

    client = s3_client if s3_client is not None else boto3.client("s3")

    try:
        response = client.get_object(
            Bucket=bucket,
            Key=key,
        )
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]

        # A missing checkpoint is normal on the first run.
        if error_code in {"NoSuchKey", "404"}:
            return None

        raise

    state = json.loads(
        response["Body"].read().decode("utf-8")
    )

    return datetime.fromisoformat(
        state["last_successful_end"]
    )


def save_s3_checkpoint(
    bucket: str,
    end: datetime,
    key: str = "dev/state/checkpoint.json",
    s3_client: Any | None = None,
) -> None:
    """Persist the latest successfully processed timestamp in S3."""

    client = s3_client if s3_client is not None else boto3.client("s3")

    state = {
        "last_successful_end": end.isoformat()
    }

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(state, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

"""Create and upload Chicago 311 ingestion manifests to S3."""

import json
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

import boto3


def upload_manifest_to_s3(
    bucket: str,
    raw_file_uri: str,
    record_count: int,
    prefix: str = "dev/manifests/",
    s3_client: Any | None = None,
) -> str:
    """Write a manifest for one successfully uploaded raw file."""

    if isinstance(record_count, bool) or not isinstance(record_count, int) or record_count < 0:
        raise ValueError("record_count must be a non-negative integer")

    parsed = urlparse(raw_file_uri)

    if (
        parsed.scheme != "s3"
        or parsed.netloc != bucket
        or not parsed.path.strip("/")
    ):
        raise ValueError(
            "raw_file_uri must refer to a file in the specified S3 bucket"
        )

    raw_key = parsed.path.lstrip("/")
    filename = PurePosixPath(raw_key).name

    if not filename.endswith(".jsonl"):
        raise ValueError("Expected a .jsonl raw file URI")

    manifest_name = filename.removesuffix(".jsonl") + ".json"

    normalized_prefix = prefix.strip("/")

    manifest_key = (
        f"{normalized_prefix}/{manifest_name}"
        if normalized_prefix
        else manifest_name
    )

    manifest = {
        "run_id": filename.removesuffix(".jsonl"),
        "created_at": datetime.now(UTC).isoformat(),
        "record_count": record_count,
        "files": [
            {
                "s3_uri": raw_file_uri,
                "s3_key": raw_key,
                "file_name": filename,
            }
        ],
    }

    client = (
        s3_client
        if s3_client is not None
        else boto3.client("s3")
    )

    client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest).encode("utf-8"),
        ContentType="application/json",
    )

    return f"s3://{bucket}/{manifest_key}"
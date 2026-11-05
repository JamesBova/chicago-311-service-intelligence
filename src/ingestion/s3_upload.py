
"""Upload Chicago 311 JSONL files to Amazon S3."""

from pathlib import Path
from typing import Any

import boto3


def upload_jsonl_to_s3(
    file_path: Path,
    bucket: str,
    prefix: str = "dev/raw/",
    s3_client: Any | None = None,
) -> str:
    """Upload a JSONL file to S3 and return its S3 URI."""

    # Verify the source file exists.
    if not file_path.is_file():
        raise FileNotFoundError(file_path)

    # Prevent accidental uploads of other file types.
    if file_path.suffix.lower() != ".jsonl":
        raise ValueError("Expected a .jsonl file")

    # Avoid uploading empty extraction files.
    if file_path.stat().st_size == 0:
        raise ValueError("Cannot upload an empty file")

    # Build the destination S3 object key.
    normalized_prefix = prefix.strip("/")
    key = (
        f"{normalized_prefix}/{file_path.name}"
        if normalized_prefix
        else file_path.name
    )

    # Use the supplied client for testing, or AWS credentials
    # automatically resolved by boto3 in normal execution.
    client = s3_client if s3_client is not None else boto3.client("s3")

    # This raises an exception if the upload fails.
    client.upload_file(
        str(file_path),
        bucket,
        key,
        ExtraArgs={"ContentType": "application/x-ndjson"},
    )

    return f"s3://{bucket}/{key}"

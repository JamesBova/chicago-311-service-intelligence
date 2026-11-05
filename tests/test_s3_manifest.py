"""Unit tests for Chicago 311 S3 manifest generation."""

import json
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from src.ingestion.s3_manifest import upload_manifest_to_s3

BUCKET = "test-chicago-311"

RAW_FILE_URI = (
    "s3://test-chicago-311/dev/raw/"
    "chicago311_20261105T080000000000_"
    "20261105T090000000000.jsonl"
)


def test_upload_manifest_success():
    """Manifest contains the correct file information and record count."""

    s3_client = Mock()

    manifest_uri = upload_manifest_to_s3(
        bucket=BUCKET,
        raw_file_uri=RAW_FILE_URI,
        record_count=1523,
        s3_client=s3_client,
    )

    s3_client.put_object.assert_called_once()

    kwargs = s3_client.put_object.call_args.kwargs

    assert kwargs["Bucket"] == BUCKET
    assert kwargs["ContentType"] == "application/json"

    assert kwargs["Key"] == (
        "dev/manifests/"
        "chicago311_20261105T080000000000_"
        "20261105T090000000000.json"
    )

    manifest = json.loads(kwargs["Body"])

    assert manifest["record_count"] == 1523

    assert manifest["run_id"] == (
        "chicago311_20261105T080000000000_"
        "20261105T090000000000"
    )

    assert len(manifest["files"]) == 1

    raw_file = manifest["files"][0]

    assert raw_file["s3_uri"] == RAW_FILE_URI

    assert raw_file["s3_key"] == (
        "dev/raw/"
        "chicago311_20261105T080000000000_"
        "20261105T090000000000.jsonl"
    )

    assert raw_file["file_name"] == (
        "chicago311_20261105T080000000000_"
        "20261105T090000000000.jsonl"
    )

    created_at = datetime.fromisoformat(manifest["created_at"])

    assert created_at.utcoffset() == datetime.now(UTC).utcoffset()

    assert manifest_uri == (
        "s3://test-chicago-311/"
        "dev/manifests/"
        "chicago311_20261105T080000000000_"
        "20261105T090000000000.json"
    )


@pytest.mark.parametrize(
    "record_count",
    [-1, 1.5, "100", None, True],
)
def test_invalid_record_count(record_count):
    """Invalid record counts must be rejected."""

    s3_client = Mock()

    with pytest.raises(ValueError):
        upload_manifest_to_s3(
            bucket=BUCKET,
            raw_file_uri=RAW_FILE_URI,
            record_count=record_count,
            s3_client=s3_client,
        )

    s3_client.put_object.assert_not_called()


def test_zero_record_count():
    """A zero record count is valid."""

    s3_client = Mock()

    upload_manifest_to_s3(
        bucket=BUCKET,
        raw_file_uri=RAW_FILE_URI,
        record_count=0,
        s3_client=s3_client,
    )

    kwargs = s3_client.put_object.call_args.kwargs

    manifest = json.loads(kwargs["Body"])

    assert manifest["record_count"] == 0


@pytest.mark.parametrize(
    "raw_file_uri",
    [
        "https://example.com/file.jsonl",
        "s3://wrong-bucket/dev/raw/file.jsonl",
        "s3://test-chicago-311/",
        "s3://test-chicago-311/dev/raw/file.csv",
    ],
)
def test_invalid_raw_file_uri(raw_file_uri):
    """Invalid raw file locations must be rejected."""

    s3_client = Mock()

    with pytest.raises(ValueError):
        upload_manifest_to_s3(
            bucket=BUCKET,
            raw_file_uri=raw_file_uri,
            record_count=100,
            s3_client=s3_client,
        )

    s3_client.put_object.assert_not_called()


def test_manifest_upload_failure():
    """S3 upload failures must propagate to the caller."""

    s3_client = Mock()

    s3_client.put_object.side_effect = RuntimeError(
        "Simulated S3 failure"
    )

    with pytest.raises(RuntimeError, match="Simulated S3 failure"):
        upload_manifest_to_s3(
            bucket=BUCKET,
            raw_file_uri=RAW_FILE_URI,
            record_count=100,
            s3_client=s3_client,
        )
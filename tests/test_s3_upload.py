
from unittest.mock import Mock

import pytest

from src.ingestion.s3_upload import upload_jsonl_to_s3

BUCKET = "james-chicago-311-service-intelligence"


def test_upload_success(tmp_path):
    file_path = tmp_path / "test.jsonl"
    file_path.write_text('{"sr_number": "SR001"}\n')

    client = Mock()

    uri = upload_jsonl_to_s3(
        file_path,
        bucket=BUCKET,
        s3_client=client,
    )

    assert uri == f"s3://{BUCKET}/dev/raw/test.jsonl"

    client.upload_file.assert_called_once_with(
        str(file_path),
        BUCKET,
        "dev/raw/test.jsonl",
        ExtraArgs={"ContentType": "application/x-ndjson"},
    )


def test_upload_custom_prefix(tmp_path):
    file_path = tmp_path / "test.jsonl"
    file_path.write_text('{"sr_number": "SR001"}\n')

    client = Mock()

    uri = upload_jsonl_to_s3(
        file_path,
        bucket=BUCKET,
        prefix="test/raw/",
        s3_client=client,
    )

    assert uri == f"s3://{BUCKET}/test/raw/test.jsonl"


def test_upload_missing_file(tmp_path):
    file_path = tmp_path / "missing.jsonl"
    client = Mock()

    with pytest.raises(FileNotFoundError):
        upload_jsonl_to_s3(
            file_path,
            bucket=BUCKET,
            s3_client=client,
        )

    client.upload_file.assert_not_called()


def test_upload_invalid_extension(tmp_path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("test")

    client = Mock()

    with pytest.raises(ValueError, match="Expected a .jsonl file"):
        upload_jsonl_to_s3(
            file_path,
            bucket=BUCKET,
            s3_client=client,
        )

    client.upload_file.assert_not_called()


def test_upload_empty_file(tmp_path):
    file_path = tmp_path / "empty.jsonl"
    file_path.touch()

    client = Mock()

    with pytest.raises(ValueError, match="empty file"):
        upload_jsonl_to_s3(
            file_path,
            bucket=BUCKET,
            s3_client=client,
        )

    client.upload_file.assert_not_called()


def test_upload_failure(tmp_path):
    file_path = tmp_path / "test.jsonl"
    file_path.write_text('{"sr_number": "SR001"}\n')

    client = Mock()
    client.upload_file.side_effect = RuntimeError(
        "Simulated S3 failure"
    )

    with pytest.raises(RuntimeError, match="Simulated S3 failure"):
        upload_jsonl_to_s3(
            file_path,
            bucket=BUCKET,
            s3_client=client,
        )


import json
from datetime import datetime
from io import BytesIO
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from src.ingestion.s3_checkpoint import (
    load_s3_checkpoint,
    save_s3_checkpoint,
)

BUCKET = "james-chicago-311-service-intelligence"
KEY = "dev/state/checkpoint.json"


def test_load_existing_checkpoint():
    timestamp = datetime(2026, 10, 31, 9, 30)

    client = Mock()
    client.get_object.return_value = {
        "Body": BytesIO(
            json.dumps(
                {"last_successful_end": timestamp.isoformat()}
            ).encode("utf-8")
        )
    }

    result = load_s3_checkpoint(
        bucket=BUCKET,
        s3_client=client,
    )

    assert result == timestamp
    client.get_object.assert_called_once_with(
        Bucket=BUCKET,
        Key=KEY,
    )


def test_load_missing_checkpoint():
    client = Mock()
    client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
        "GetObject",
    )

    assert load_s3_checkpoint(BUCKET, s3_client=client) is None


def test_load_permission_error():
    client = Mock()
    client.get_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
        "GetObject",
    )

    with pytest.raises(ClientError):
        load_s3_checkpoint(BUCKET, s3_client=client)


def test_save_checkpoint():
    timestamp = datetime(2026, 10, 31, 9, 30)
    client = Mock()

    save_s3_checkpoint(
        bucket=BUCKET,
        end=timestamp,
        s3_client=client,
    )

    client.put_object.assert_called_once()

    kwargs = client.put_object.call_args.kwargs

    assert kwargs["Bucket"] == BUCKET
    assert kwargs["Key"] == KEY
    assert kwargs["ContentType"] == "application/json"

    state = json.loads(kwargs["Body"].decode("utf-8"))

    assert state["last_successful_end"] == timestamp.isoformat()


def test_save_failure_propagates():
    client = Mock()
    client.put_object.side_effect = RuntimeError(
        "Simulated S3 checkpoint failure"
    )

    with pytest.raises(
        RuntimeError,
        match="Simulated S3 checkpoint failure",
    ):
        save_s3_checkpoint(
            bucket=BUCKET,
            end=datetime(2026, 10, 31, 9, 30),
            s3_client=client,
        )

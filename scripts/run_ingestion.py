"""Run Chicago 311 incremental ingestion."""

import argparse
from datetime import UTC, datetime
from pathlib import Path

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests

from src.ingestion.pipeline import run_ingestion
from src.ingestion.s3_checkpoint import (
    load_s3_checkpoint,
    save_s3_checkpoint,
)
from src.ingestion.s3_manifest import upload_manifest_to_s3
from src.ingestion.s3_upload import upload_jsonl_to_s3
from src.ingestion.source_time import to_source_time


def main():
    parser = argparse.ArgumentParser(
        description="Run Chicago 311 incremental ingestion"
    )

    parser.add_argument(
        "--initial-start",
        required=True,
        help="Initial Chicago local timestamp, e.g. 2026-10-31T08:00:00",
    )

    parser.add_argument(
        "--page-size",
        type=int,
        default=200,
    )

    parser.add_argument(
        "--s3-bucket",
        help="S3 bucket for raw files and persistent checkpoint",
    )

    parser.add_argument(
        "--s3-prefix",
        default="dev/raw/",
        help="S3 prefix for raw files (default: dev/raw/)",
    )

    parser.add_argument(
        "--s3-manifest-prefix",
        default="dev/manifests/",
        help="S3 prefix for manifests (default: dev/manifests/)",
    )

    parser.add_argument(
        "--s3-checkpoint-key",
        default="dev/state/checkpoint.json",
        help="S3 key for persistent checkpoint",
    )

    args = parser.parse_args()

    initial_start = datetime.fromisoformat(args.initial_start)

    if initial_start.tzinfo is not None:
        parser.error(
            "Initial start must be Chicago local time without an offset"
        )

    if args.page_size <= 0:
        parser.error("Page size must be greater than zero")

    current_time = to_source_time(datetime.now(UTC))

    project_root = Path(__file__).resolve().parents[1]

    checkpoint_path = (
        project_root / "data" / "state" / "checkpoint.json"
    )

    raw_directory = project_root / "data" / "raw"

    upload_file = None
    load_state = None
    save_state = None
    save_manifest = None

    if args.s3_bucket:
        bucket = args.s3_bucket
        prefix = args.s3_prefix
        checkpoint_key = args.s3_checkpoint_key
        manifest_prefix = args.s3_manifest_prefix

        def upload_file(file_path: Path) -> str:
            uri = upload_jsonl_to_s3(
                file_path=file_path,
                bucket=bucket,
                prefix=prefix,
            )

            print(f"Uploaded to S3: {uri}")
            return uri

        def save_manifest(
            raw_file_uri: str,
            record_count: int,
        ) -> None:
            uri = upload_manifest_to_s3(
                bucket=bucket,
                raw_file_uri=raw_file_uri,
                record_count=record_count,
                prefix=manifest_prefix,
            )

            print(f"Manifest uploaded: {uri}")

        def load_state() -> datetime | None:
            return load_s3_checkpoint(
                bucket=bucket,
                key=checkpoint_key,
            )

        def save_state(value: datetime) -> None:
            save_s3_checkpoint(
                bucket=bucket,
                end=value,
                key=checkpoint_key,
            )

    print(f"Current Chicago time: {current_time}")
    print(f"Initial start: {initial_start}")

    if args.s3_bucket:
        print(f"S3 bucket: {args.s3_bucket}")
        print(f"S3 raw prefix: {args.s3_prefix}")
        print(f"S3 manifest prefix: {args.s3_manifest_prefix}")
        print(f"S3 checkpoint: {args.s3_checkpoint_key}")
    else:
        print("S3 upload: disabled")
        print(f"Local checkpoint: {checkpoint_path}")

    print("Starting ingestion...")

    with requests.Session() as session:

        retry_policy = Retry(
            total=4,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry_policy)
        session.mount("https://", adapter)

        result = run_ingestion(
            session=session,
            checkpoint_path=checkpoint_path,
            raw_directory=raw_directory,
            initial_start=initial_start,
            current_time=current_time,
            page_size=args.page_size,
            upload_file=upload_file,
            load_state=load_state,
            save_state=save_state,
            save_manifest=save_manifest,
        )

    if result is None:
        print("No new extraction window available.")
        return

    record_count, destination = result

    print("Ingestion completed successfully.")
    print(f"Records extracted: {record_count}")
    print(f"Raw file: {destination}")


if __name__ == "__main__":
    main()
"""Chicago 311 processing DAG: Bronze -> dbt Silver -> dbt Gold.

Airflow 3.3 / CeleryExecutor. Replaces the earlier connectivity-only DAG while
keeping the same DAG ID and the existing ``snowflake_chicago311`` connection.

IMPORTANT SETUP (not performed by this file):
  - This DAG is MANUAL ONLY. Trigger it only after the existing ECS -> S3 ->
    Snowpipe Bronze load has completed and its manifest has been reconciled.
    It never consumes the Bronze stream or launches ingestion.
  - Install dbt Core + dbt-snowflake in BOTH Airflow worker images.
  - Mount the Chicago 311 dbt project into BOTH workers at /opt/airflow/dbt
    (or set CHICAGO311_DBT_PROJECT_DIR). The project must contain
    dbt_project.yml, models/silver/*.sql and models/gold/*.sql.
  - The dbt models must resolve to CHICAGO_311_DEV.SILVER and
    CHICAGO_311_DEV.GOLD, respectively. The DAG checks that before dbt writes.
    dbt's *default* custom-schema behavior often adds a prefix; configure
    your dbt schema-name macro appropriately.
  - dbt authentication is prepared at task runtime from the existing Airflow
    Snowflake connection. No extra password, key or profiles.yml in Git.

The existing cluster smoke-test DAG is not modified. Do not schedule this DAG
or connect the ingestion trigger until model SQL, tests, and completion gating
have been validated.
"""

import base64
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

LOG = logging.getLogger(__name__)
SNOWFLAKE_CONN_ID = "snowflake_chicago311"
DATABASE = "CHICAGO_311_DEV"
WAREHOUSE = "DE_PORTFOLIO_WH"
ROLE = "CHICAGO_311_TRANSFORM_ROLE"
USER = "CHICAGO_311_AIRFLOW_SVC"
DBT_DIR = Path(os.getenv("CHICAGO311_DBT_PROJECT_DIR", "/opt/airflow/dbt"))

GOLD_TABLES = (
    "DAILY_REQUEST_METRICS",
    "DIM_DATE",
    "DIM_GEOGRAPHY",
    "DIM_SERVICE_TYPE",
    "FACT_SERVICE_REQUEST",
)


def _hook():
    # Import at worker execution time, not during scheduler/DAG parsing.
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


def _count_rows(relation: str) -> int:
    # Only pass constant, fully-qualified relation names defined in this file.
    row = _hook().get_first(f"SELECT COUNT(*) FROM {relation}")
    if row is None:
        raise RuntimeError(f"No count result from {relation}")
    return int(row[0])


def _extra(extra: dict, field: str):
    # Both unprefixed and legacy provider-prefixed forms are supported.
    return extra.get(field) or extra.get(f"extra__snowflake__{field}")


def _run_command(command: list[str], env: dict[str, str], timeout: int) -> str:
    # Never log the profile contents, private key or key passphrase.
    completed = subprocess.run(
        command,
        cwd=str(DBT_DIR),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    # dbt output can be useful for troubleshooting; do not enable debug logs
    # without reviewing them for potentially sensitive configuration details.
    if completed.returncode != 0:
        LOG.error("dbt command failed (exit %s): %s", completed.returncode, command[1:])
        LOG.error("dbt output (last 5000 chars): %s", completed.stdout[-5000:])
        raise RuntimeError("dbt failed; inspect task logs for the dbt error")
    return completed.stdout


def _run_dbt_layer(layer: str) -> None:
    """Build only this layer's models and tests, using ephemeral credentials."""
    if layer not in ("silver", "gold"):
        raise ValueError("Unsupported layer")

    if not shutil.which("dbt"):
        raise RuntimeError(
            "dbt CLI is missing in this Airflow WORKER container. "
            "Install dbt-snowflake in both worker images before running this DAG."
        )
    project_file = DBT_DIR / "dbt_project.yml"
    model_dir = DBT_DIR / "models" / layer
    if not project_file.is_file() or not model_dir.is_dir():
        raise RuntimeError(
            f"Chicago 311 dbt project not mounted at {DBT_DIR}; "
            "expected dbt_project.yml and models/{layer}/"
        )
    if not any(model_dir.rglob("*.sql")):
        raise RuntimeError(f"No dbt SQL models found in {model_dir}; refusing a no-op build")

    # PyYAML is available with Airflow/dbt. The profile name is taken from the
    # real dbt project; we do not guess a name or require another credentials file.
    import yaml

    with project_file.open(encoding="utf-8") as fp:
        project = yaml.safe_load(fp)
    profile_name = (project or {}).get("profile") or (project or {}).get("name")
    if not profile_name:
        raise RuntimeError("dbt_project.yml has no project name or profile")

    conn = _hook().get_connection(SNOWFLAKE_CONN_ID)
    extra = conn.extra_dejson
    account = _extra(extra, "account")
    raw_key = _extra(extra, "private_key_content")
    if not account or not raw_key or not conn.password:
        raise RuntimeError(
            "snowflake_chicago311 must have account, private_key_content "
            "and encrypted-key passphrase (Password) configured"
        )

    # The Airflow connection holds Base64-encoded PEM data from the user's .p8.
    # Support an unencoded PEM as well, for compatibility with provider formats.
    try:
        key_bytes = (
            raw_key.encode("utf-8")
            if "-----BEGIN" in raw_key
            else base64.b64decode("".join(raw_key.split()), validate=True)
        )
    except (ValueError, base64.binascii.Error) as exc:
        raise RuntimeError("Airflow connection private_key_content is not valid Base64") from exc

    if b"-----BEGIN" not in key_bytes:
        raise RuntimeError("Expected a PKCS#8 PEM private key in the Airflow connection")

    # Temporary files are private to the worker task and removed at exit.
    with tempfile.TemporaryDirectory(prefix="chicago311-dbt-") as temp:
        temp_path = Path(temp)
        key_path = temp_path / "private_key.p8"
        key_path.write_bytes(key_bytes)
        key_path.chmod(0o600)

        profile = {
            profile_name: {
                "target": "airflow",
                "outputs": {
                    "airflow": {
                        "type": "snowflake",
                        "account": account,
                        "user": conn.login,
                        "role": ROLE,
                        "warehouse": WAREHOUSE,
                        "database": DATABASE,
                        "schema": "SILVER",
                        "threads": 2,
                        "private_key_path": str(key_path),
                        "private_key_passphrase": conn.password,
                        "client_session_keep_alive": False,
                    }
                },
            }
        }
        profile_path = temp_path / "profiles.yml"
        profile_path.write_text(yaml.safe_dump(profile), encoding="utf-8")
        profile_path.chmod(0o600)
        if str(conn.login).upper() != USER:
            raise RuntimeError("Snowflake connection user is not the expected service user")

        # dbt writes only temporary artifacts to this worker's private directory.
        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = str(temp_path)
        env["DBT_TARGET_PATH"] = str(temp_path / "target")
        env["DBT_LOG_PATH"] = str(temp_path / "logs")

        select = f"path:models/{layer}"
        common = [
            "--project-dir", str(DBT_DIR),
            "--profiles-dir", str(temp_path),
            "--target", "airflow",
        ]

        # Prevent dbt's default schema prefix behavior from silently writing
        # SILVER_GOLD instead of GOLD, or to a different database/schema.
        inspect_output = _run_command(
            ["dbt", "ls", *common, "--select", select, "--resource-type", "model",
             "--output", "json", "--output-keys", "name", "resource_type", "database", "schema"],
            env,
            timeout=300,
        )
        nodes = []
        for line in inspect_output.splitlines():
            if line.strip().startswith("{"):
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if obj.get("resource_type") == "model":
                    nodes.append(obj)
        if not nodes:
            raise RuntimeError(f"dbt selected no {layer} models; refusing a no-op run")
        for node in nodes:
            if (str(node.get("database") or "").upper() != DATABASE or
                    str(node.get("schema") or "").upper() != layer.upper()):
                raise RuntimeError(
                    f"dbt model {node.get('name')} would write to "
                    f"{node.get('database')}.{node.get('schema')} instead of "
                    f"{DATABASE}.{layer.upper()}. Fix schema mapping first."
                )
        LOG.info("Validated %d dbt %s model destinations", len(nodes), layer)

        if layer == "gold":
            seed_output = _run_command(
                [
                    "dbt", "ls", *common,
                    "--select", "community_areas",
                    "--resource-type", "seed",
                    "--output", "json",
                    "--output-keys", "name", "resource_type", "database", "schema",
                ],
                env,
                timeout=300,
            )

            seed_nodes = []
            for line in seed_output.splitlines():
                if line.strip().startswith("{"):
                    try:
                        node = json.loads(line)
                        if node.get("resource_type") == "seed":
                            seed_nodes.append(node)
                    except json.JSONDecodeError:
                        continue

            if (
                len(seed_nodes) != 1
                or str(seed_nodes[0].get("database", "")).upper() != DATABASE
                or str(seed_nodes[0].get("schema", "")).upper() != "GOLD"
            ):
                raise RuntimeError(
                    "Community areas seed must target the expected GOLD schema"
                )

            _run_command(
                ["dbt", "seed", *common, "--select", "community_areas"],
                env,
                timeout=600,
            )

            import csv

            seed_file = DBT_DIR / "seeds" / "community_areas.csv"
            with seed_file.open(encoding="utf-8-sig", newline="") as fp:
                expected_count = sum(1 for _ in csv.DictReader(fp))

            actual_count = _count_rows(f"{DATABASE}.GOLD.COMMUNITY_AREAS")
            if expected_count == 0 or actual_count != expected_count:
                raise RuntimeError(
                    f"Community areas seed count mismatch: "
                    f"expected {expected_count}, found {actual_count}"
                )

            LOG.info("Community areas seed verified: %s rows", actual_count)


        build_output = _run_command(
            ["dbt", "build", *common, "--select", select],
            env,
            timeout=3600,
        )

        for line in build_output.splitlines():
            if "PASS=" in line and "TOTAL=" in line:
                LOG.info("dbt %s summary: %s", layer, line.strip())

        LOG.info(
            "dbt %s build and selected tests finished successfully",
            layer,
        )


@dag(
    dag_id="chicago_311_processing_pipeline",
    description="Chicago 311: validate Bronze, dbt build Silver, dbt build Gold",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,  # manual until ingestion completion is integrated
    catchup=False,
    max_active_runs=1,
    tags=["chicago-311", "snowflake", "dbt", "bronze-silver-gold"],
)
def chicago_311_processing_pipeline():
    @task(retries=0)
    def build_silver() -> None:
        _run_dbt_layer("silver")

    @task(retries=1, retry_delay=timedelta(minutes=1))
    def verify_silver() -> None:
        count = _count_rows(f"{DATABASE}.SILVER.SERVICE_REQUEST_CURRENT")
        if count <= 0:
            raise RuntimeError("Silver SERVICE_REQUEST_CURRENT is empty after dbt build")
        LOG.info("Silver current records: %s", count)

    @task(retries=0)
    def build_gold() -> None:
        _run_dbt_layer("gold")

    @task(retries=1, retry_delay=timedelta(minutes=1))
    def verify_gold() -> None:
        counts = {
            table: _count_rows(f"{DATABASE}.GOLD.{table}")
            for table in GOLD_TABLES
        }
        for name, count in counts.items():
            LOG.info("Gold %s: %s rows", name, count)
        if counts["FACT_SERVICE_REQUEST"] <= 0:
            raise RuntimeError("Gold FACT_SERVICE_REQUEST is empty after dbt build")
        if counts["DAILY_REQUEST_METRICS"] <= 0:
            raise RuntimeError("Gold DAILY_REQUEST_METRICS is empty after dbt build")

    @task(retries=1, retry_delay=timedelta(minutes=1))
    def cleanup_bronze() -> None:
        _hook().run(
            f"DELETE FROM {DATABASE}.BRONZE.SERVICE_REQUEST_RAW "
            "WHERE INGESTED_AT < DATEADD(day, -7, CURRENT_TIMESTAMP())"
        )
        LOG.info("Bronze cleanup completed")

    @task(retries=1, retry_delay=timedelta(minutes=1))
    def cleanup_gold() -> None:
        _hook().run(
            f"DELETE FROM {DATABASE}.GOLD.FACT_SERVICE_REQUEST "
            "WHERE CREATED_DATE < DATEADD(MONTH, -12, CURRENT_DATE())"
        )
        _hook().run(
            f"DELETE FROM {DATABASE}.GOLD.DAILY_REQUEST_METRICS "
            "WHERE METRIC_DATE < DATEADD(MONTH, -12, CURRENT_DATE())"
        )
        LOG.info("Gold cleanup completed")
    # Strict dependency order; a failure prevents all downstream writes.
    build_silver() >> verify_silver() >> build_gold() >> verify_gold() >> cleanup_bronze() >> cleanup_gold()


chicago_311_processing_pipeline()

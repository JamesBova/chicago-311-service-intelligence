# Chicago 311 — distributed Airflow development environment

This Compose project simulates a multi-component Airflow 3 deployment on **one Windows PC**. It does not provide genuine multi-host high availability or production security. It does not modify ECS, S3, Snowpipe, the Snowflake triggered task, Lambda, or existing Bronze data.

## Components

- Airflow 3.3.0 API server (localhost:8080)
- Two Airflow schedulers (HA *simulation*)
- One DAG processor
- Two Celery workers (concurrency 2 each)
- One triggerer
- Redis 7.2 as Celery broker
- PostgreSQL 16 as metadata database and Celery result backend
- One-shot `airflow-init` for DB migration and admin user
- Optional Flower monitoring UI (localhost:5555)

Metadata is in a named Docker volume (`postgres-data`). Redis persistence is a separate named volume. Do not confuse volumes with backups: deleting a volume destroys its data. All containers share the same single Docker host, and the DB and broker are single-instance services.

## Installation (PowerShell, from this folder)

1. Copy `.env.example` to `.env`:

   ```powershell
   Copy-Item .env.example .env
   ```

2. Edit `.env`. Generate a valid Fernet key and JWT secret using Python:

   ```powershell
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   python -c "import secrets; print(secrets.token_urlsafe(48))"
   ```

   If `cryptography` isn't installed locally, use `py -m pip install cryptography`, or generate the key inside a temporary Airflow image. Use long, distinct, random values for the PostgreSQL and web admin passwords. Do **not** commit `.env`.

3. Give Docker Desktop approximately 8-12 GB of available RAM initially (more if needed). On Windows/WSL2 this may require modifying WSL memory limits. This setup runs multiple Python processes and is intentionally heavier than a minimal dev stack.

4. Validate the Compose configuration and start:

   ```powershell
   docker compose config --quiet
   docker compose up -d
   docker compose ps
   ```

5. Open `http://localhost:8080` and sign in with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env`. If services fail to initialize, inspect:

   ```powershell
   docker compose logs --tail=100 airflow-init airflow-api-server airflow-worker-1
   ```

6. To enable Flower:

   ```powershell
   docker compose --profile monitoring up -d
   ```

   View `http://localhost:5555` **locally only**. Flower is an additional local monitoring tool, not a secure public endpoint.

## Verify worker and scheduler distribution

Open the Airflow UI and manually trigger `chicago_311_cluster_smoke_test`. It has four independent probe tasks that print worker container hostnames. Inspect the task logs and Flower for which workers handled work. The broker decides distribution; a single run is not guaranteed to use both workers.

To exercise failure handling, start several smoke-test runs and stop a worker while work is queued:

```powershell
docker compose stop airflow-worker-1
docker compose ps
# Observe whether the surviving worker drains queued tasks. In-flight tasks may need retries.
docker compose start airflow-worker-1
```

To test scheduler redundancy, stop `airflow-scheduler-1` and manually trigger a new smoke-test run while `airflow-scheduler-2` is healthy. Restore it afterwards.

**Important:** If you hard-kill a worker, Celery can temporarily leave tasks pending or require retry/visibility-timeout handling. Two workers improve availability but cannot guarantee no task interruption or exactly-once execution. Silver and Gold transformations must be safe to retry (idempotent).

## Operational precautions

- **Do not run** `docker compose down -v` unless you intend to delete PostgreSQL and Redis volumes.
- Take real backups with `pg_dump` (and store them outside Docker), plus source-control DAGs, configuration and dbt models. Test restoring a backup.
- Airflow logs live in `./logs` for this local environment. Real multi-host deployments need centralized/remote logs.
- Both API server and Flower are bound to `127.0.0.1`. Lambda in AWS **cannot** reach this Airflow environment over `localhost`. Do not open port 8080 publicly as a quick fix.
- Avoid committing private Snowflake keys or passwords. The future Airflow-to-Snowflake connection should use the `CHICAGO_311_AIRFLOW_SVC` service user/key pair, scoped to Bronze read and Silver/Gold write.

## Next implementation phases

1. **Infrastructure:** bring up distributed Airflow; test both schedulers and workers; document resource use.
2. **Snowflake connection:** securely configure Airflow's service-user credentials and verify it can read the **committed Bronze transient table**. The existing Lambda process already consumes the original stream, so do not have a second consumer advance that same stream.
3. **dbt model:** write incremental Bronze → Silver (`SERVICE_REQUEST_CURRENT`) then dimension/fact/metrics models in Gold. Use unique keys/merge logic to prevent duplicates after retries. Add uniqueness, not-null and count checks.
4. **DAG:** implement a manually triggerable DAG orchestrating dbt and validation. Establish a precise processed-batch ID or file watermark so a retry is safe.
5. **AWS integration:** once Airflow is hosted on an AWS-accessible private endpoint, update Lambda to trigger the DAG **after** validation and commit. Use an authenticated API and retry/idempotency policy. For local-only development, manually trigger the DAG or test via a secure, explicitly managed development relay.
6. **Recovery:** simulate worker and scheduler failure, metadata backup/restore, container restart, and duplicate DAG triggering. Record what is (and is not) resilient on one host.

## Version scope

Pinned `apache/airflow:3.3.0` to match the project's earlier development version. The configuration follows the Airflow 3 CeleryExecutor Compose pattern. Image startup and remote integrations require testing on your Docker Desktop; this package has been statically validated but not run against your environment.

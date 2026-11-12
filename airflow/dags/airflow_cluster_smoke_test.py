"""Manual validation DAG for the local distributed Airflow setup."""

from datetime import datetime, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="chicago_311_cluster_smoke_test",
    schedule=None,
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["chicago-311", "infrastructure"],
)
def cluster_smoke_test():
    @task
    def probe():
        import socket
        import time

        worker_host = socket.gethostname()
        print(f"Task ran on worker host: {worker_host}")
        time.sleep(12)
        return worker_host

    # Four parallel tasks. With per-worker concurrency=2, workers can share the work.
    for index in range(1, 5):
        probe.override(task_id=f"worker_probe_{index}")()


cluster_smoke_test()

"""Bonus (Project_2.md 2.5A): refresh a DuckDB copy of gold and compare it
against PostgreSQL on two business queries.

Not scheduled on its own -- triggered automatically by f1_ingestion's last
task via TriggerDagRunOperator(wait_for_completion=False), so this DAG's
success or failure can never affect f1_ingestion's own status (see README
"DuckDB bonus"). Can also be triggered manually at any time.
"""

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DUCKDB_ENVIRONMENT = {
    "PGHOST": "warehouse",
    "PGPORT": "5432",
    "PGDATABASE": os.environ.get("PGDATABASE", ""),
    "PGUSER": os.environ.get("PGUSER", ""),
    "PGPASSWORD": os.environ.get("PGPASSWORD", ""),
}
# No host path is available here (Docker-outside-of-Docker), so this uses the
# named volume rather than the ./04_duckdb/data bind mount that manual
# `docker compose run` invocations use -- see compose.yaml's `duckdb-data`.
DUCKDB_MOUNTS = [Mount(source="f1-duckdb-data", target="/data", type="volume")]


def duckdb_task(task_id, command):
    return DockerOperator(
        task_id=task_id,
        image=os.environ["DUCKDB_IMAGE"],
        command=command,
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=os.environ["COMPOSE_NETWORK"],
        mount_tmp_dir=False,
        environment=DUCKDB_ENVIRONMENT,
        mounts=DUCKDB_MOUNTS,
    )


@dag(
    dag_id="f1_duckdb_refresh",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Tallinn"),
    schedule=None,  # triggered by f1_ingestion, or manually
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["f1", "bonus", "duckdb"],
)
def f1_duckdb_refresh():
    refresh_duckdb = duckdb_task("refresh_duckdb", ["python", "refresh_duckdb.py"])
    compare_engines = duckdb_task("compare_engines", ["python", "compare_engines.py"])
    refresh_duckdb >> compare_engines


f1_duckdb_refresh()

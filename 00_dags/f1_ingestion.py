"""Project 2 sections 2.1/2.2/2.4: parameterized OpenF1 -> PostgreSQL bronze,
then dbt build -> silver/gold, in one DAG.

Fully automatic by default: plan_sessions discovers every completed round for
each configured season from OpenF1's own calendar and skips anything already
in bronze, so a weekly run only fetches genuinely new rounds instead of
re-downloading the whole season each time. One task group per (round,
session) pair: extract -> check -> load. Mapping per session means one failed
extraction doesn't block any other session's load (see ingest_session.expand
below). After all sessions are attempted, build_warehouse runs `dbt build`
(as a sibling container -- see README "dbt automation") to refresh silver and
gold from whatever is now in bronze, even if some sessions failed. Finally,
trigger_duckdb_refresh fires the separate f1_duckdb_refresh DAG (bonus,
Project_2.md 2.5A) without waiting for it, so that DAG's outcome can never
affect this one's status -- see README "DuckDB bonus". The last task,
report_status, fails the run if any earlier task failed, so partial refreshes
still happen but a failed ingestion is never reported as success.
"""

import os
from datetime import date, timedelta

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator

@dag(
    dag_id="f1_ingestion",
    # Airflow interprets a cron schedule in the DAG's timezone, taken from
    # start_date's tzinfo -- pendulum/Airflow handle the DST transitions.
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Tallinn"),
    schedule="0 7 * * 1",  # every Monday 07:00 Europe/Tallinn
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "seasons": Param([2025, 2026], type="array", minItems=1, uniqueItems=True,
                         items={"type": "integer", "minimum": 2023, "maximum": 2100}),
        "sessions": Param(["Q", "R"], type="array", minItems=1, uniqueItems=True,
                          items={"type": "string", "enum": ["Q", "R"]}),
        "cutoff": Param(None, type=["null", "string"], format="date",
                        description="Exclusive UTC date. Empty uses the DAG interval end date."),
    },
    tags=["f1", "bronze", "postgresql"],
)
def f1_ingestion():
    @task
    def plan_sessions():
        import psycopg

        from ingestion.openf1 import OpenF1Client, schedule
        from ingestion.quality import validate_parameters

        context = get_current_context()
        params = context["params"]
        cutoff = params["cutoff"] or context["data_interval_end"].date().isoformat()
        cutoff_date = date.fromisoformat(cutoff)

        # Skip batches already in bronze so a weekly run only touches new
        # rounds. To force a refresh of an already-loaded session, use the
        # CLI directly: `ingestion.cli ingest --season ... --round ... --session ...`.
        with psycopg.connect("") as connection:
            loaded = set(connection.execute(
                "SELECT DISTINCT season, round_number, session_type FROM bronze.session_results"
            ).fetchall())

        client = OpenF1Client()
        batches = []
        for season in params["seasons"]:
            # rounds=[1] is a placeholder: validate_parameters only checks
            # season/sessions/cutoff here, since rounds are discovered below.
            config = validate_parameters(season, [1], params["sessions"], cutoff)
            _, races = schedule(client, config["season"])
            for round_number, race in enumerate(races, 1):
                if date.fromisoformat(race["date_start"][:10]) >= cutoff_date:
                    continue  # race weekend hasn't happened yet
                for session_type in config["sessions"]:
                    key = (config["season"], round_number, session_type)
                    if key in loaded:
                        continue
                    batches.append({"season": config["season"], "round_number": round_number,
                                    "session_type": session_type, "cutoff": cutoff})
        return batches

    @task_group(group_id="ingest_session")
    def ingest_session(parameters):
        @task(max_active_tis_per_dag=1, execution_timeout=timedelta(minutes=15))
        def extract(parameters):
            # Serialized across all mapped instances: each extract spins up its
            # own OpenF1Client with its own independent request-rate throttle,
            # so running several concurrently would still blow past OpenF1's
            # actual rate limit (observed: 429 Too Many Requests) even though
            # each individual client self-throttles to one request per ~2.2s.
            from ingestion.extract import extract_session
            return extract_session(**parameters)

        @task
        def check(payload):
            from ingestion.quality import KNOWN_MISSING_LAPS, check_dataset
            meta = payload["metadata"]
            check_dataset("laps", payload["laps"],
                          allow_empty=(meta["season"], meta["round_number"], meta["session_type"]) in KNOWN_MISSING_LAPS)
            check_dataset("session_results", payload["session_results"])
            return payload

        @task
        def load(payload):
            from ingestion.load_bronze import load_session
            return load_session(payload)

        load(check(extract(parameters)))

    build_warehouse = DockerOperator(
        task_id="build_warehouse",
        image=os.environ["DBT_IMAGE"],
        command=["build"],
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=os.environ["COMPOSE_NETWORK"],
        mount_tmp_dir=False,
        # Same environment as the `dbt` service in compose.yaml -- this
        # container is the same image, just launched by Airflow instead of
        # `docker compose run --rm dbt build`.
        environment={
            "PGHOST": "warehouse",
            "PGPORT": "5432",
            "PGDATABASE": os.environ["PGDATABASE"],
            "PGUSER": os.environ["PGUSER"],
            "PGPASSWORD": os.environ["PGPASSWORD"],
            "DBT_PROFILES_DIR": "/project/dbt",
            "DBT_TARGET_PATH": "/tmp/dbt-target",
            "DBT_LOG_PATH": "/tmp/dbt-logs",
        },
        # Rebuild gold from whatever made it into bronze even if some
        # sessions failed (e.g. a transient OpenF1 outage on one session
        # shouldn't block the rest of the data from reaching gold).
        trigger_rule="all_done",
    )

    # Fire-and-forget: this only tells Airflow to start f1_duckdb_refresh and
    # succeeds immediately. It never waits for or checks that DAG's outcome,
    # so a bonus-feature failure there can never mark this DAG run failed.
    trigger_duckdb_refresh = TriggerDagRunOperator(
        task_id="trigger_duckdb_refresh",
        trigger_dag_id="f1_duckdb_refresh",
        wait_for_completion=False,
        trigger_rule="all_done",
    )

    # Airflow derives a run's final state from its leaf tasks only. Because
    # build_warehouse and trigger_duckdb_refresh use all_done (so partial data
    # still refreshes gold), a failed plan_sessions/extract/load would otherwise
    # leave a green leaf and the run would report success. This last task keeps
    # the partial refresh but fails the run if anything upstream failed.
    @task(trigger_rule="all_done", retries=0)
    def report_status():
        dag_run = get_current_context()["dag_run"]
        failed = sorted({
            f"{ti.task_id}[{ti.map_index}]" if ti.map_index >= 0 else ti.task_id
            for ti in dag_run.get_task_instances()
            if ti.task_id != "report_status" and ti.state in ("failed", "upstream_failed")
        })
        if failed:
            raise AirflowException(
                f"{len(failed)} task(s) failed or were blocked by a failure: {', '.join(failed)}"
            )

    ingest_session.expand(parameters=plan_sessions()) >> build_warehouse >> trigger_duckdb_refresh >> report_status()


f1_ingestion()

"""Project 2 sections 2.1/2.2: parameterized OpenF1 -> PostgreSQL bronze.

Fully automatic by default: plan_sessions discovers every completed round for
each configured season from OpenF1's own calendar and skips anything already
in bronze, so a weekly run only fetches genuinely new rounds instead of
re-downloading the whole season each time. One task group per (round,
session) pair: extract -> check -> load. Mapping per session means one failed
extraction doesn't block any other session's load (see ingest_session.expand
below).
"""

from datetime import date, timedelta

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.operators.python import get_current_context


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
                    if (config["season"], round_number, session_type) in loaded:
                        continue  # already loaded
                    batches.append({"season": config["season"], "round_number": round_number,
                                    "session_type": session_type, "cutoff": cutoff})
        return batches

    @task_group(group_id="ingest_session")
    def ingest_session(parameters):
        @task(execution_timeout=timedelta(minutes=15))
        def extract(parameters):
            from ingestion.extract import extract_session
            return extract_session(**parameters)

        @task
        def check(payload):
            from ingestion.quality import check_dataset
            check_dataset("laps", payload["laps"])
            check_dataset("session_results", payload["session_results"])
            return payload

        @task
        def load(payload):
            from ingestion.load_bronze import load_session
            return load_session(payload)

        load(check(extract(parameters)))

    ingest_session.expand(parameters=plan_sessions())


f1_ingestion()

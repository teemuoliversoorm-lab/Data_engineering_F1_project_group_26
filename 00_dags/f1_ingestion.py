"""Project 2 sections 2.1/2.2: parameterized OpenF1 -> PostgreSQL bronze.

One task group per (round, session) pair: extract -> check -> load. Mapping
per session means one failed extraction doesn't block any other session's
load (see ingest_session.expand below).
"""

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.operators.python import get_current_context


@dag(
    dag_id="f1_ingestion",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 6 * * 2",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "season": Param(2025, type="integer", minimum=2023, maximum=2100),
        "rounds": Param([1], type="array", minItems=1, uniqueItems=True,
                        items={"type": "integer", "minimum": 1, "maximum": 30}),
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
        from ingestion.quality import validate_parameters
        context = get_current_context()
        params = context["params"]
        cutoff = params["cutoff"] or context["data_interval_end"].date().isoformat()
        config = validate_parameters(params["season"], params["rounds"], params["sessions"], cutoff)
        return [{"season": config["season"], "round_number": r, "session_type": s, "cutoff": cutoff}
                for r in config["rounds"] for s in config["sessions"]]

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

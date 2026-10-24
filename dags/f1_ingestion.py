"""Project 2 sections 2.1/2.2: parameterized OpenF1 -> PostgreSQL bronze."""

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
        "allow_missing_laps": Param(False, type="boolean",
                                  description="Load available results with an explicit source gap; never invent lap rows."),
        "reuse_landed": Param(False, type="boolean",
                             description="Resume from latest validated historical bundle; do not refresh existing files."),
    },
    tags=["f1", "bronze", "postgresql"],
)
def f1_ingestion():
    @task
    def plan_sessions():
        from ingestion.contracts import validate_parameters
        context = get_current_context()
        params = context["params"]
        cutoff = params["cutoff"] or context["data_interval_end"].date().isoformat()
        config = validate_parameters(params["season"], params["rounds"], params["sessions"], cutoff)
        batches = [{"season": config["season"], "round_number": r, "session_type": s, "cutoff": cutoff}
                   for r in config["rounds"] for s in config["sessions"]]
        if params["reuse_landed"]:
            for batch in batches:
                batch["reuse_landed"] = True
        if params["allow_missing_laps"]:
            for batch in batches:
                batch["allow_missing_laps"] = True
        return batches

    @task_group(group_id="ingest_session")
    def ingest_session(parameters):
        @task(max_active_tis_per_dag=1, execution_timeout=timedelta(minutes=30))
        def extract(parameters):
            from ingestion.extract import extract_session
            return extract_session(**parameters)

        @task
        def check(manifest_path):
            from ingestion.load_bronze import read_bundle
            manifest, _ = read_bundle(manifest_path)
            print({name: spec["rows"] for name, spec in manifest["files"].items()})
            return manifest_path

        @task
        def load(manifest_path):
            from ingestion.load_bronze import load_bundle
            return load_bundle(manifest_path)

        load(check(extract(parameters)))

    # A mapped group makes each check/load depend only on its own extraction.
    # Mapping task outputs separately introduces a barrier across all sessions.
    ingest_session.expand(parameters=plan_sessions())


f1_ingestion()

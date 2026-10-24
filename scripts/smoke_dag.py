"""Execute the real Airflow DAG locally, including dynamic tasks, without a scheduler."""

import argparse

import pendulum
from airflow.models import DagBag

parser = argparse.ArgumentParser()
parser.add_argument("--season", type=int, default=2025)
parser.add_argument("--rounds", type=int, nargs="+", default=[1])
parser.add_argument("--sessions", nargs="+", default=["Q", "R"])
parser.add_argument("--cutoff", default="2025-03-18")
parser.add_argument("--retries", type=int, default=0,
                    help="Local diagnostic retries; defaults to 0. Scheduled DAG keeps its retries.")
parser.add_argument("--reuse-landed", action="store_true",
                    help="Resume from validated saved bundles; only missing sessions contact OpenF1.")
parser.add_argument("--allow-missing-laps", action="store_true",
                    help="Load available session results and explicitly audit unavailable lap data.")
args = parser.parse_args()
if args.retries < 0:
    parser.error("--retries must be nonnegative")
bag = DagBag(dag_folder="dags", include_examples=False)
if bag.import_errors:
    raise RuntimeError(bag.import_errors)
dag = bag.get_dag("f1_ingestion")
for task in dag.tasks:
    task.retries = args.retries
config = vars(args).copy()
config.pop("retries")
run = dag.test(execution_date=pendulum.now("UTC"), run_conf=config)
if run.state != "success":
    raise RuntimeError(f"DAG did not succeed: {run.state}")
import psycopg
with psycopg.connect("") as connection:
    gaps = connection.execute(
        """SELECT round_number,session_type,dataset,metadata->>'session_key',
               metadata->'dataset_status'->>dataset
        FROM bronze.load_audit WHERE season=%s AND round_number=ANY(%s)
        AND session_type=ANY(%s) AND metadata->'dataset_status'->>dataset IN ('unavailable','partial')
        ORDER BY round_number,session_type,dataset""",
        (args.season, args.rounds, args.sessions),
    ).fetchall()
if gaps:
    print(f"DAG completed with explicitly recorded source gaps: {gaps}")
else:
    print("DAG smoke test succeeded; no requested source gaps recorded")

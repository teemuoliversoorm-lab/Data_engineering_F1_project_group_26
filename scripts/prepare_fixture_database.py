"""Create an isolated fixture database and execute the real mapped Airflow DAG.

This script never writes synthetic records to the configured warehouse database.
The chosen new database must not already exist. Drop it after dbt verification.
"""

import argparse
import os
import re
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import pendulum
import psycopg
from psycopg import sql
from airflow.models import DagBag

from ingestion.load_bronze import initialize_database
from tests.fixtures import bundle

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--database", default="f1_storage_test")
args = parser.parse_args()
if not re.fullmatch(r"f1_[a-z0-9_]+_test", args.database):
    raise ValueError("Test database name must match f1_[a-z0-9_]+_test")

with psycopg.connect("", autocommit=True) as admin:
    admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(args.database)))
with psycopg.connect("", dbname=args.database, autocommit=True) as connection:
    initialize_database(connection)

with tempfile.TemporaryDirectory() as root:
    def extract_fixture(season, round_number, session_type, cutoff):
        return bundle(Path(root), name=uuid.uuid4().hex, session=session_type)
    bag = DagBag(dag_folder="dags", include_examples=False)
    if bag.import_errors:
        raise RuntimeError(bag.import_errors)
    dag = bag.get_dag("f1_ingestion")
    with patch.dict(os.environ, {"PGDATABASE": args.database}), \
         patch("ingestion.extract.extract_session", side_effect=extract_fixture):
        config = {"season": 2025, "rounds": [1], "sessions": ["Q", "R"], "cutoff": "2025-03-18"}
        for attempt in range(2):
            run = dag.test(execution_date=pendulum.now("UTC"), run_conf=config)
            if run.state != "success":
                raise RuntimeError(f"Fixture DAG run {attempt + 1} failed: {run.state}")
            with psycopg.connect("", dbname=args.database, autocommit=True) as connection:
                snapshot = connection.execute(
                    "SELECT dataset, session_type, content_sha256, loaded_at FROM bronze.load_audit ORDER BY dataset, session_type"
                ).fetchall()
                if attempt == 0:
                    previous = snapshot
                elif snapshot != previous:
                    raise AssertionError("Identical DAG rerun changed bronze audit state")
print(f"Two fixture DAG runs succeeded in {args.database}; warehouse remains unchanged")

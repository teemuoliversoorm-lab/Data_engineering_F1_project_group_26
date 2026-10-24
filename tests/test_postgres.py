"""Integration tests use a newly created temporary database, never warehouse tables."""

import os
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ingestion.extract import write_bundle
from ingestion.load_bronze import initialize_database, load_bundle
from tests.fixtures import METADATA, bundle, datasets


@unittest.skipUnless(os.environ.get("F1_TEST_POSTGRES") == "1", "Set F1_TEST_POSTGRES=1 for PostgreSQL tests")
class PostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        cls.psycopg = psycopg
        cls.database = "f1_test_" + uuid.uuid4().hex
        cls.admin = psycopg.connect("", autocommit=True)
        cls.admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database)))
        cls.connection = psycopg.connect("", dbname=cls.database, autocommit=True)
        initialize_database(cls.connection)

    @classmethod
    def tearDownClass(cls):
        from psycopg import sql
        cls.connection.close()
        cls.admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(cls.database)))
        cls.admin.close()

    def setUp(self):
        self.connection.execute("TRUNCATE bronze.laps, bronze.session_results, bronze.load_audit, bronze.source_responses")
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def snapshot(self):
        return {
            table: self.connection.execute(f"SELECT to_jsonb(t) FROM bronze.{table} t ORDER BY to_jsonb(t)::text").fetchall()
            for table in ("laps", "session_results", "load_audit", "source_responses")
        }

    def test_identical_rerun_is_noop(self):
        path = bundle(self.root)
        self.assertEqual(load_bundle(path, self.connection)["status"], "loaded")
        before = self.snapshot()
        self.assertEqual(load_bundle(path, self.connection)["status"], "unchanged")
        self.assertEqual(before, self.snapshot())

    def test_new_extraction_timestamp_is_noop(self):
        load_bundle(bundle(self.root), self.connection)
        before = self.snapshot()
        metadata = dict(METADATA, extracted_at="2025-03-19T06:00:00+00:00", cutoff="2025-03-19")
        result = load_bundle(bundle(self.root, "later", metadata=metadata), self.connection)
        self.assertEqual(result["status"], "unchanged")
        self.assertEqual(before, self.snapshot())

    def test_event_metadata_correction_reloads(self):
        load_bundle(bundle(self.root), self.connection)
        metadata = dict(METADATA, event_name="Corrected Fixture Grand Prix")
        result = load_bundle(bundle(self.root, "metadata", metadata=metadata), self.connection)
        self.assertEqual(result["status"], "loaded")
        self.assertEqual(self.connection.execute(
            "SELECT DISTINCT metadata->>'event_name' FROM bronze.session_results"
        ).fetchall(), [("Corrected Fixture Grand Prix",)])

    def test_correction_removes_obsolete_rows_preserves_other_sessions(self):
        load_bundle(bundle(self.root, "q", session="Q"), self.connection)
        load_bundle(bundle(self.root, "r"), self.connection)
        data = datasets()
        data["laps"] = [data["laps"][0]]
        data["session_results"] = [data["session_results"][0]]
        data["session_results"][0]["Points"] = 24
        load_bundle(bundle(self.root, "corrected", data=data), self.connection)
        counts = self.connection.execute(
            "SELECT session_type, count(*) FROM bronze.laps GROUP BY session_type ORDER BY session_type"
        ).fetchall()
        self.assertEqual(counts, [("Q", 4), ("R", 1)])
        self.assertEqual(self.connection.execute(
            "SELECT (source_record->>'Points')::int FROM bronze.session_results WHERE session_type='R'"
        ).fetchone()[0], 24)

    def test_database_failure_rolls_back_both_datasets(self):
        load_bundle(bundle(self.root), self.connection)
        before = self.snapshot()
        self.connection.execute(
            "ALTER TABLE bronze.laps ADD CONSTRAINT fixture_failure CHECK (source_record->>'TrackStatus' <> 'force_failure')"
        )
        try:
            data = datasets()
            data["laps"][0]["TrackStatus"] = "force_failure"
            data["session_results"][0]["Points"] = 1
            with self.assertRaises(self.psycopg.errors.CheckViolation):
                load_bundle(bundle(self.root, "bad", data=data), self.connection)
            self.assertEqual(before, self.snapshot())
        finally:
            self.connection.execute("ALTER TABLE bronze.laps DROP CONSTRAINT fixture_failure")

    def test_concurrent_same_batch_serializes(self):
        path = bundle(self.root)
        def run():
            with self.psycopg.connect("", dbname=self.database, autocommit=True) as connection:
                return load_bundle(path, connection)["status"]
        with ThreadPoolExecutor(max_workers=2) as executor:
            statuses = list(executor.map(lambda _: run(), range(2)))
        self.assertEqual(sorted(statuses), ["loaded", "unchanged"])
        self.assertEqual(self.connection.execute("SELECT count(*) FROM bronze.laps").fetchone()[0], 4)


    def source_bundle(self, name, value=1, data=None):
        responses = {"laps?session_key=102": {
            "url": "https://api.openf1.org/v1/laps?session_key=102",
            "records": [{"driver_number": 1, "lap_number": 1, "source_value": value}],
        }}
        return write_bundle(self.root / name, dict(METADATA, provider="OpenF1"),
                            data or datasets(), raw_responses=responses)

    def test_raw_source_is_stored_and_identical_replay_is_noop(self):
        path = self.source_bundle("source")
        self.assertEqual(load_bundle(path, self.connection)["status"], "loaded")
        before = self.snapshot()
        self.assertEqual(load_bundle(path, self.connection)["status"], "unchanged")
        self.assertEqual(before, self.snapshot())
        self.assertEqual(self.connection.execute(
            "SELECT source_records->0->>'source_value' FROM bronze.source_responses"
        ).fetchone()[0], "1")

    def test_raw_only_correction_reloads_in_same_batch(self):
        load_bundle(self.source_bundle("source"), self.connection)
        self.assertEqual(load_bundle(self.source_bundle("correction", value=2), self.connection)["status"], "loaded")
        self.assertEqual(self.connection.execute(
            "SELECT source_records->0->>'source_value' FROM bronze.source_responses"
        ).fetchone()[0], "2")

    def test_raw_source_rolls_back_with_failed_lap_write(self):
        load_bundle(self.source_bundle("source"), self.connection)
        before = self.snapshot()
        self.connection.execute(
            "ALTER TABLE bronze.laps ADD CONSTRAINT raw_fixture_failure CHECK (source_record->>'TrackStatus' <> 'force_failure')"
        )
        try:
            data = datasets()
            data["laps"][0]["TrackStatus"] = "force_failure"
            with self.assertRaises(self.psycopg.errors.CheckViolation):
                load_bundle(self.source_bundle("bad", value=2, data=data), self.connection)
            self.assertEqual(before, self.snapshot())
        finally:
            self.connection.execute("ALTER TABLE bronze.laps DROP CONSTRAINT raw_fixture_failure")



    def partial_bundle(self, name):
        data = datasets()
        data["laps"] = []
        metadata = dict(METADATA, provider="OpenF1",
                        dataset_status={"laps": "unavailable", "session_results": "available"})
        raw = {"laps?session_key=102": {
            "url": "https://api.openf1.org/v1/laps?session_key=102", "records": [],
        }}
        return write_bundle(self.root / name, metadata, data, raw_responses=raw)

    def test_partial_load_is_explicit_idempotent_and_can_be_completed(self):
        path = self.partial_bundle("partial")
        result = load_bundle(path, self.connection)
        self.assertEqual(result["status"], "loaded_with_gaps")
        self.assertEqual(result["missing_datasets"], ["laps"])
        self.assertEqual(self.connection.execute("SELECT count(*) FROM bronze.laps").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM bronze.session_results").fetchone()[0], 2)
        self.assertEqual(self.connection.execute(
            "SELECT row_count,metadata->'dataset_status'->>'laps' FROM bronze.load_audit WHERE dataset='laps'"
        ).fetchone(), (0, "unavailable"))
        before = self.snapshot()
        self.assertEqual(load_bundle(path, self.connection)["status"], "unchanged")
        self.assertEqual(before, self.snapshot())
        self.assertEqual(load_bundle(self.source_bundle("complete"), self.connection)["status"], "loaded")
        self.assertEqual(self.connection.execute("SELECT count(*) FROM bronze.laps").fetchone()[0], 4)

    def test_partial_source_cannot_erase_previously_loaded_laps(self):
        from ingestion.contracts import DataQualityError
        load_bundle(self.source_bundle("complete"), self.connection)
        before = self.snapshot()
        with self.assertRaisesRegex(DataQualityError, "cannot replace previously loaded"):
            load_bundle(self.partial_bundle("partial"), self.connection)
        self.assertEqual(before, self.snapshot())

    def test_failed_extract_does_not_block_other_session_load(self):
        import os
        import pendulum
        from unittest.mock import patch
        from airflow.models import DagBag
        from ingestion.openf1 import SourceUnavailableError
        bag = DagBag(dag_folder="dags", include_examples=False)
        self.assertFalse(bag.import_errors)
        dag = bag.get_dag("f1_ingestion")
        for task in dag.tasks:
            task.retries = 0

        def source(season, round_number, session_type, cutoff):
            if session_type == "Q":
                raise SourceUnavailableError("Fixture unavailable qualifying laps")
            return bundle(self.root, name="successful_race", session="R")

        with patch.dict(os.environ, {"PGDATABASE": self.database}), \
             patch("ingestion.extract.extract_session", side_effect=source):
            run = dag.test(
                execution_date=pendulum.now("UTC"),
                run_conf={"season": 2025, "rounds": [1], "sessions": ["Q", "R"],
                          "cutoff": "2025-03-18"},
            )
        self.assertEqual(run.state, "failed")
        self.assertEqual(self.connection.execute(
            "SELECT DISTINCT session_type FROM bronze.session_results"
        ).fetchall(), [("R",)])
        self.assertEqual(self.connection.execute("SELECT count(*) FROM bronze.laps").fetchone()[0], 4)



    def test_missing_classification_is_audited_as_partial_with_null_rank(self):
        from ingestion.extract import extract_session
        from tests.test_extract import FixtureClient
        path = extract_session(2026, 1, "Q", "2026-10-22", data_dir=self.root,
                               client=FixtureClient(unclassified_participant=True))
        result = load_bundle(path, self.connection)
        self.assertEqual(result["status"], "loaded_with_gaps")
        self.assertEqual(result["partial_datasets"], ["session_results"])
        self.assertEqual(result["participants_without_classification"], [18])
        self.assertEqual(self.connection.execute(
            """SELECT source_record->>'Position', source_record->>'ClassificationAvailable'
            FROM bronze.session_results WHERE driver_number='18'"""
        ).fetchone(), (None, "false"))
        self.assertEqual(self.connection.execute(
            """SELECT metadata->'dataset_status'->>'session_results'
            FROM bronze.load_audit WHERE dataset='session_results'"""
        ).fetchone()[0], "partial")
        before = self.snapshot()
        self.assertEqual(load_bundle(path, self.connection)["status"], "unchanged")
        self.assertEqual(before, self.snapshot())


if __name__ == "__main__":
    unittest.main()

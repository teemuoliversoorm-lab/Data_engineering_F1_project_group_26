"""Minimal tests covering what Project 2 asks the pipeline to demonstrate:

1. the data-quality check rejects bad data (test_quality.py-style checks)
2. the bronze loader is idempotent (running it twice doesn't duplicate rows)
3. the OpenF1 field mapping produces the columns silver/gold expect

Run with:
    docker compose run --rm -e F1_TEST_POSTGRES=1 pipeline -m unittest tests.test_pipeline -v
"""

import os
import unittest
import uuid

from ingestion.openf1 import adapt
from ingestion.quality import DataQualityError, check_dataset

# A tiny but complete session payload, reused by the idempotency test below.
SAMPLE_PAYLOAD = {
    "metadata": {
        "season": 2025, "round_number": 1, "session_type": "R", "cutoff": "2026-01-01",
        "provider": "OpenF1", "event_name": "Test Grand Prix", "country": "Testland",
        "circuit_name": "Test Circuit", "session_date": "2025-03-16", "event_date": "2025-03-16",
    },
    "laps": [{
        "DriverNumber": "1", "LapNumber": 1, "LapTime": "PT90.5S", "Position": None,
        "TrackStatus": None, "PositionMethod": None, "PositionObservedAt": None,
    }],
    "session_results": [{
        "DriverNumber": "1", "DriverId": "openf1:test_driver", "TeamId": "openf1:test_team",
        "FullName": "Test Driver", "Abbreviation": "TST", "TeamName": "Test Team", "CountryCode": "TST",
        "Position": 1, "PositionStatusCode": None, "ClassificationAvailable": True,
        "GridPosition": 1, "Q1": None, "Q2": None, "Q3": None, "Points": 25, "Status": "Finished",
    }],
}


class DataQualityTests(unittest.TestCase):
    """Project 2 requires at least one data-quality check in the pipeline."""

    def test_valid_batch_is_accepted(self):
        self.assertEqual(check_dataset("session_results", SAMPLE_PAYLOAD["session_results"]), 1)

    def test_position_outside_the_grid_is_rejected(self):
        bad_row = dict(SAMPLE_PAYLOAD["session_results"][0], Position=99)
        with self.assertRaises(DataQualityError):
            check_dataset("session_results", [bad_row])

    def test_duplicate_driver_is_rejected(self):
        row = SAMPLE_PAYLOAD["session_results"][0]
        with self.assertRaises(DataQualityError):
            check_dataset("session_results", [row, row])

    def test_missing_required_column_is_rejected(self):
        incomplete_row = {"DriverNumber": "1"}
        with self.assertRaises(DataQualityError):
            check_dataset("session_results", [incomplete_row])


class OpenF1AdaptTests(unittest.TestCase):
    """adapt() must produce exactly the field names 02_dbt/models/silver/ expects."""

    def test_adapt_maps_driver_and_lap_fields(self):
        drivers = [{"driver_number": 1, "full_name": "Max Verstappen",
                   "name_acronym": "VER", "team_name": "Red Bull Racing", "country_code": "NED"}]
        raw_results = [{"driver_number": 1, "position": 1, "points": 25}]
        raw_laps = [{"driver_number": 1, "lap_number": 1, "lap_duration": 90.5,
                    "date_start": "2025-03-16T05:00:00+00:00"}]

        datasets = adapt("R", raw_laps, raw_results, drivers, grid=[], positions=[])

        result = datasets["session_results"][0]
        self.assertEqual(result["DriverId"], "openf1:max_verstappen")
        self.assertEqual(result["TeamId"], "openf1:red_bull_racing")
        self.assertEqual(result["Status"], "Finished")
        self.assertTrue(result["ClassificationAvailable"])

        lap = datasets["laps"][0]
        self.assertEqual(lap["LapTime"], "PT90.5S")
        self.assertIsNone(lap["Position"])  # no position samples supplied

    def test_adapt_uses_dsq_dnf_dns_flags_when_position_is_null(self):
        """Regression test: OpenF1 can report dsq=true with position=null (a
        post-race disqualification, e.g. Leclerc/Hamilton at the 2025 Chinese
        GP) rather than a string status code. Status must still be set."""
        drivers = [{"driver_number": 16, "full_name": "Charles Leclerc",
                   "name_acronym": "LEC", "team_name": "Ferrari", "country_code": None}]
        raw_results = [{"driver_number": 16, "position": None, "points": 0.0,
                        "dnf": False, "dns": False, "dsq": True}]

        datasets = adapt("R", raw_laps=[], raw_results=raw_results, drivers=drivers, grid=[], positions=[])

        result = datasets["session_results"][0]
        self.assertEqual(result["Status"], "Disqualified")
        self.assertIsNone(result["Position"])


@unittest.skipUnless(os.environ.get("F1_TEST_POSTGRES") == "1", "Set F1_TEST_POSTGRES=1 for PostgreSQL tests")
class IdempotencyTests(unittest.TestCase):
    """Project 2 requires that re-running the pipeline for the same input
    does not create duplicates or fail."""

    def test_loading_the_same_session_twice_does_not_duplicate_rows(self):
        import psycopg
        from psycopg import sql

        from ingestion.load_bronze import initialize_database, load_session

        database = "f1_test_" + uuid.uuid4().hex
        admin = psycopg.connect("", autocommit=True)
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        try:
            connection = psycopg.connect("", dbname=database, autocommit=True)
            initialize_database(connection)

            load_session(SAMPLE_PAYLOAD, connection)
            load_session(SAMPLE_PAYLOAD, connection)  # identical rerun

            laps = connection.execute("SELECT count(*) FROM bronze.laps").fetchone()[0]
            results = connection.execute("SELECT count(*) FROM bronze.session_results").fetchone()[0]
            self.assertEqual((laps, results), (1, 1))
            connection.close()
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
            admin.close()


if __name__ == "__main__":
    unittest.main()

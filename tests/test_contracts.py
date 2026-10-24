import json
import tempfile
import unittest
from pathlib import Path

from ingestion.contracts import DataQualityError, validate_dataset, validate_pair, validate_parameters
from ingestion.load_bronze import read_bundle
from tests.fixtures import bundle, datasets


class ContractTests(unittest.TestCase):
    def test_valid_pair(self):
        data = datasets()
        validate_pair(data["laps"], data["session_results"])

    def test_duplicate_lap_rejected(self):
        data = datasets()["laps"]
        data.append(data[0].copy())
        with self.assertRaisesRegex(DataQualityError, "duplicate"):
            validate_dataset("laps", data)

    def test_null_driver_id_rejected(self):
        data = datasets()["session_results"]
        data[0]["DriverId"] = None
        with self.assertRaisesRegex(DataQualityError, "DriverId"):
            validate_dataset("session_results", data)

    def test_duplicate_driver_identity_rejected(self):
        data = datasets()["session_results"]
        data[1]["DriverId"] = data[0]["DriverId"]
        with self.assertRaisesRegex(DataQualityError, "duplicate DriverId"):
            validate_dataset("session_results", data)

    def test_empty_rejected(self):
        with self.assertRaisesRegex(DataQualityError, "empty"):
            validate_dataset("laps", [])

    def test_out_of_range_position_rejected(self):
        data = datasets()["laps"]
        data[0]["Position"] = 25
        with self.assertRaisesRegex(DataQualityError, "outside"):
            validate_dataset("laps", data)

    def test_fractional_lap_rejected(self):
        data = datasets()["laps"]
        data[0]["LapNumber"] = 1.5
        with self.assertRaisesRegex(DataQualityError, "integer"):
            validate_dataset("laps", data)

    def test_pitlane_and_missing_metrics_allowed(self):
        data = datasets()["session_results"]
        data[0].update(GridPosition=0, Position=None, Q1=None, Q2=None, Q3=None)
        validate_dataset("session_results", data)

    def test_orphan_lap_rejected(self):
        data = datasets()
        data["laps"][0]["DriverNumber"] = "99"
        with self.assertRaisesRegex(DataQualityError, "absent"):
            validate_pair(data["laps"], data["session_results"])

    def test_parameters(self):
        config = validate_parameters(2025, [2, 1, 1], ["R", "Q"], "2025-04-01")
        self.assertEqual(config["rounds"], [1, 2])
        for args in [(2022, [1], ["R"], "2025-01-01"),
                     (2025, [], ["R"], "2025-01-01"),
                     (2025, [1], ["S"], "2025-01-01"),
                     (2025, [1], ["R"], "not-a-date")]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                validate_parameters(*args)

    def test_checksum_and_count(self):
        with tempfile.TemporaryDirectory() as root:
            manifest_path = bundle(Path(root))
            manifest, _ = read_bundle(manifest_path)
            self.assertEqual(manifest["files"]["laps"]["rows"], 4)
            source = Path(manifest_path).parent / "laps.json"
            source.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(DataQualityError, "checksum"):
                read_bundle(manifest_path)

    def test_missing_dataset_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(bundle(Path(root)))
            manifest = json.loads(path.read_text())
            del manifest["files"]["laps"]
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(DataQualityError, "both datasets"):
                read_bundle(path)


if __name__ == "__main__":
    unittest.main()

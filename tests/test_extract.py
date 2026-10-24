import copy
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError, URLError

from ingestion.contracts import DataQualityError
from ingestion.extract import extract_session
from ingestion.load_bronze import read_bundle
from ingestion.openf1 import OpenF1Client, SourceUnavailableError, adapt

Q = {"session_key": 101, "meeting_key": 10, "session_name": "Qualifying",
     "date_start": "2025-03-15T05:00:00+00:00", "date_end": "2025-03-15T06:00:00+00:00"}
R = {"session_key": 102, "meeting_key": 10, "session_name": "Race",
     "date_start": "2025-03-16T04:00:00+00:00", "date_end": "2025-03-16T06:00:00+00:00"}
DRIVER = {"driver_number": 4, "full_name": "Lando NORRIS", "name_acronym": "NOR",
          "team_name": "McLaren", "country_code": None}
LAP = {"driver_number": 4, "lap_number": 1, "lap_duration": 90.5,
       "date_start": "2025-03-16T04:00:00+00:00", "session_key": 102}
RESULT = {"driver_number": 4, "position": 1, "duration": 5400, "points": 25,
          "dnf": False, "dns": False, "dsq": False}
GRID = {"driver_number": 4, "position": 2}
POSITION = {"driver_number": 4, "position": 1, "date": "2025-03-16T04:01:00+00:00"}


class FixtureClient:
    def __init__(self, empty_laps=False, duplicate_driver=False, cutoff_race=None, unclassified_participant=False):
        self.responses = {}
        self.calls = []
        self.empty_laps = empty_laps
        self.duplicate_driver = duplicate_driver
        self.cutoff_race = cutoff_race
        self.unclassified_participant = unclassified_participant

    def get(self, endpoint, allow_empty=False, **filters):
        self.calls.append((endpoint, filters))
        key = filters.get("session_key")
        if endpoint == "sessions":
            rows = [Q, self.cutoff_race or R,
                    dict(Q, session_key=100, session_name="Sprint Qualifying")]
        elif endpoint == "meetings":
            rows = [{"meeting_name": "Australian Grand Prix", "country_name": "Australia",
                     "circuit_short_name": "Melbourne"}]
        elif endpoint == "laps":
            rows = [] if self.empty_laps else [LAP]
            if self.unclassified_participant:
                rows = rows + [dict(LAP, driver_number=18)]
        elif endpoint == "session_result":
            rows = [dict(RESULT, duration=[82.0, 81.0, 80.0])] if key == 101 else [RESULT]
        elif endpoint == "drivers":
            rows = [DRIVER, DRIVER] if self.duplicate_driver else [DRIVER]
            if self.unclassified_participant:
                rows = rows + [dict(DRIVER, driver_number=18, full_name="Lance STROLL", team_name="Aston Martin")]
        elif endpoint == "position":
            rows = [POSITION]
        elif endpoint == "starting_grid":
            rows = [] if key == 102 else [GRID]
        else:
            raise AssertionError(endpoint)
        rows = copy.deepcopy(rows)
        query = endpoint + "?" + str(sorted(filters.items()))
        self.responses[query] = {"url": "https://api.openf1.org/v1/" + endpoint, "records": rows}
        return rows


class ExtractTests(unittest.TestCase):
    def test_race_bundle_has_raw_provenance_and_grid_fallback(self):
        with tempfile.TemporaryDirectory() as root:
            client = FixtureClient()
            path = extract_session(2025, 1, "R", "2025-03-18", data_dir=root, client=client)
            manifest, data = read_bundle(path)
            self.assertEqual(manifest["metadata"]["provider"], "OpenF1")
            self.assertEqual(manifest["metadata"]["session_key"], 102)
            self.assertEqual(manifest["metadata"]["grid_session_key"], 101)
            self.assertEqual(data["session_results"][0]["DriverId"], "openf1:lando_norris")
            self.assertEqual(data["session_results"][0]["GridPosition"], 2)
            self.assertEqual(data["session_results"][0]["Points"], 25)
            self.assertEqual(data["laps"][0]["LapTime"], "PT90.5S")
            self.assertEqual(data["laps"][0]["Position"], 1)
            self.assertEqual(data["laps"][0]["_openf1"], LAP)
            self.assertTrue((Path(path).parent / "openf1_raw.json").is_file())

    def test_qualifying_stages_use_result_array_not_lap_minimum(self):
        with tempfile.TemporaryDirectory() as root:
            path = extract_session(2025, 1, "Q", "2025-03-18", data_dir=root, client=FixtureClient())
            _, data = read_bundle(path)
            self.assertEqual([data["session_results"][0][key] for key in ("Q1", "Q2", "Q3")],
                             ["PT82.0S", "PT81.0S", "PT80.0S"])
            self.assertIsNone(data["session_results"][0]["GridPosition"])
            untimed = adapt("Q", [], [dict(RESULT, duration=None)], [DRIVER], [], [])
            self.assertEqual([untimed["session_results"][0][key] for key in ("Q1", "Q2", "Q3")],
                             [None, None, None])

    def test_cutoff_checks_session_end_before_fetching_laps(self):
        with tempfile.TemporaryDirectory() as root:
            client = FixtureClient(cutoff_race=dict(R, date_end="2025-03-18T00:10:00+00:00"))
            with self.assertRaisesRegex(DataQualityError, "cutoff"):
                extract_session(2025, 1, "R", "2025-03-18", data_dir=root, client=client)
            self.assertEqual([item[0] for item in client.calls], ["sessions"])
            self.assertEqual(list(Path(root).iterdir()), [])

    def test_empty_laps_and_duplicate_driver_never_publish(self):
        for options in ({"empty_laps": True}, {"duplicate_driver": True}):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as root:
                with self.assertRaises(DataQualityError):
                    extract_session(2025, 1, "R", "2025-03-18", data_dir=root,
                                    client=FixtureClient(**options))
                self.assertEqual(list(Path(root).iterdir()), [])

    def test_raw_checksum_is_checked(self):
        with tempfile.TemporaryDirectory() as root:
            path = extract_session(2025, 1, "R", "2025-03-18", data_dir=root, client=FixtureClient())
            (Path(path).parent / "openf1_raw.json").write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(DataQualityError, "raw checksum"):
                read_bundle(path)

    def test_missing_metrics_remain_null_and_future_position_is_not_used(self):
        data = adapt("R", [dict(LAP, lap_duration=None), dict(LAP, lap_number=2)], [dict(RESULT, points=None)],
                     [DRIVER], [], [dict(POSITION, date="2025-03-16T06:00:00+00:00")])
        self.assertIsNone(data["laps"][0]["Position"])
        self.assertIsNone(data["laps"][0]["LapTime"])
        self.assertIsNone(data["laps"][1]["Position"])
        self.assertIsNone(data["laps"][0]["TrackStatus"])
        self.assertIsNone(data["session_results"][0]["Points"])
        self.assertIsNone(data["session_results"][0]["GridPosition"])

    def test_driver_identity_survives_number_change(self):
        before = adapt("R", [], [RESULT], [DRIVER], [], [])["session_results"][0]
        after = adapt("R", [], [dict(RESULT, driver_number=1)],
                      [dict(DRIVER, driver_number=1)], [], [])["session_results"][0]
        self.assertEqual(before["DriverId"], after["DriverId"])


    def test_reuse_validates_landed_bundle_without_api_calls(self):
        with tempfile.TemporaryDirectory() as root:
            path = extract_session(2025, 1, "R", "2025-03-18", data_dir=root, client=FixtureClient())
            client = Mock()
            client.get.side_effect = AssertionError("Unexpected network request during resume")
            resumed = extract_session(2025, 1, "R", "2026-01-01", data_dir=root,
                                      client=client, reuse_landed=True)
            self.assertEqual(resumed, path)
            client.get.assert_not_called()

    def test_reuse_honors_stricter_cutoff(self):
        with tempfile.TemporaryDirectory() as root:
            extract_session(2025, 1, "R", "2025-03-18", data_dir=root, client=FixtureClient())
            with self.assertRaisesRegex(DataQualityError, "cutoff"):
                extract_session(2025, 1, "R", "2025-03-16", data_dir=root,
                                client=Mock(), reuse_landed=True)

    def test_reuse_rejects_corrupt_latest_bundle(self):
        with tempfile.TemporaryDirectory() as root:
            path = extract_session(2025, 1, "R", "2025-03-18", data_dir=root, client=FixtureClient())
            (Path(path).parent / "laps.json").write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(DataQualityError, "checksum"):
                extract_session(2025, 1, "R", "2026-01-01", data_dir=root,
                                client=Mock(), reuse_landed=True)

    def test_opt_in_missing_laps_preserves_results_and_flags_gap(self):
        with tempfile.TemporaryDirectory() as root:
            path = extract_session(2025, 1, "Q", "2025-03-18", data_dir=root,
                                   client=FixtureClient(empty_laps=True), allow_missing_laps=True)
            manifest, data = read_bundle(path)
            self.assertEqual(data["laps"], [])
            self.assertEqual(len(data["session_results"]), 1)
            self.assertEqual(manifest["metadata"]["dataset_status"],
                             {"laps": "unavailable", "session_results": "available"})
            self.assertEqual(manifest["files"]["laps"]["rows"], 0)
            with self.assertRaisesRegex(DataQualityError, "missing laps"):
                extract_session(2025, 1, "Q", "2025-03-18", data_dir=root,
                                client=Mock(), reuse_landed=True)


    def test_textual_retirement_code_is_preserved_without_numeric_rank(self):
        from ingestion.contracts import validate_pair
        data = adapt("Q", [LAP], [dict(RESULT, position="RT", duration=[None, None, None])],
                     [DRIVER], [], [])
        result = data["session_results"][0]
        self.assertIsNone(result["Position"])
        self.assertEqual(result["Status"], "Retired")
        self.assertEqual(result["PositionStatusCode"], "RT")
        self.assertEqual(result["_openf1"]["position"], "RT")
        self.assertTrue(result["ClassificationAvailable"])
        validate_pair(data["laps"], data["session_results"])

    def test_lap_participant_without_classification_is_explicitly_flagged(self):
        with tempfile.TemporaryDirectory() as root:
            path = extract_session(2026, 1, "Q", "2026-10-22", data_dir=root,
                                   client=FixtureClient(unclassified_participant=True))
            manifest, data = read_bundle(path)
            self.assertEqual(manifest["metadata"]["dataset_status"]["session_results"], "partial")
            self.assertEqual(manifest["metadata"]["source_classification_rows"], 1)
            self.assertEqual(manifest["metadata"]["participants_without_classification"], [18])
            participant = next(row for row in data["session_results"] if row["DriverNumber"] == "18")
            self.assertFalse(participant["ClassificationAvailable"])
            for field in ("Position", "Points", "Status", "Q1", "Q2", "Q3"):
                self.assertIsNone(participant[field])
            self.assertIsNone(participant["_openf1"])
            self.assertEqual(participant["_openf1_driver"]["full_name"], "Lance STROLL")
            self.assertEqual(len(data["laps"]), 2)

    def test_unknown_position_code_is_rejected(self):
        with self.assertRaisesRegex(DataQualityError, "unknown classification position"):
            adapt("R", [], [dict(RESULT, position="UNKNOWN")], [DRIVER], [], [])


class ClientTests(unittest.TestCase):
    def response(self, value):
        return io.StringIO(json.dumps(value))

    def test_success_and_stable_response_order(self):
        client = OpenF1Client(interval=0, opener=Mock(return_value=self.response([{"id": 2}, {"id": 1}])))
        self.assertEqual(client.get("drivers", session_key=102), [{"id": 1}, {"id": 2}])
        self.assertIn("drivers?session_key=102", client.responses)

    def test_optional_404_only_accepts_no_results_response(self):
        for optional, body in ((True, '{"detail":"No results found."}'),
                               (False, '{"detail":"No results found."}'),
                               (True, '{"detail":"Not Found"}')):
            with self.subTest(optional=optional, body=body):
                error = HTTPError("url", 404, "Not found", {}, io.BytesIO(body.encode()))
                client = OpenF1Client(interval=0, opener=Mock(side_effect=error))
                if optional and "No results found." in body:
                    self.assertEqual(client.get("starting_grid", allow_empty=True), [])
                else:
                    with self.assertRaises(SourceUnavailableError):
                        client.get("starting_grid", allow_empty=optional)

    def test_rate_limit_retries_and_honors_retry_after(self):
        error = HTTPError("url", 429, "Rate limit", {"Retry-After": "3"}, io.BytesIO(b"busy"))
        sleep = Mock()
        client = OpenF1Client(interval=0, sleep=sleep,
                              opener=Mock(side_effect=[error, self.response([{"id": 1}])]))
        self.assertEqual(client.get("drivers"), [{"id": 1}])
        sleep.assert_called_once_with(3.0)

    def test_network_retries_are_bounded(self):
        opener = Mock(side_effect=URLError("offline"))
        client = OpenF1Client(interval=0, attempts=2, sleep=Mock(), opener=opener)
        with self.assertRaises(SourceUnavailableError):
            client.get("laps")
        self.assertEqual(opener.call_count, 2)

    def test_unauthorized_is_not_retried(self):
        opener = Mock(side_effect=HTTPError("url", 401, "Unauthorized", {}, io.BytesIO(b"auth")))
        client = OpenF1Client(interval=0, opener=opener)
        with self.assertRaisesRegex(SourceUnavailableError, "401"):
            client.get("laps")
        self.assertEqual(opener.call_count, 1)

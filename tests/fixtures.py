"""Small synthetic inputs. These are test data, never observed F1 results."""

from copy import deepcopy

from ingestion.extract import write_bundle

METADATA = {
    "season": 2025, "round_number": 1, "session_type": "R",
    "cutoff": "2025-03-18", "extracted_at": "2025-03-18T06:00:00+00:00",
    "provider": "synthetic-test-fixture", "adapter_version": "fixture",
    "event_name": "Fixture Grand Prix", "country": "Test",
    "circuit_name": "Test Circuit", "session_date": "2025-03-16",
    "event_date": "2025-03-16",
}


def datasets():
    results = [
        {
            "DriverNumber": str(number), "DriverId": f"fixture_driver_{number}",
            "TeamId": "fixture_team", "TeamName": "Fixture Team",
            "FullName": f"Fixture Driver {number}", "Abbreviation": f"FD{number}",
            "CountryCode": "TST", "Position": number, "GridPosition": number,
            "Q1": "P0DT0H1M22S", "Q2": "P0DT0H1M21S", "Q3": "P0DT0H1M20S",
            "Points": 25 if number == 1 else 18, "Status": "Finished",
        } for number in (1, 2)
    ]
    laps = [
        {
            "DriverNumber": str(number), "Driver": f"FD{number}", "Team": "Fixture Team",
            "LapNumber": lap, "LapTime": "P0DT0H1M30S", "Position": number,
            "TrackStatus": "1", "Time": f"P0DT0H{lap}M30S",
        } for number in (1, 2) for lap in (1, 2)
    ]
    return {"laps": laps, "session_results": results}


def bundle(root, name="batch", session="R", data=None, metadata=None):
    meta = deepcopy(metadata or METADATA)
    meta["session_type"] = session
    if session == "Q":
        meta["session_date"] = "2025-03-15"
    return write_bundle(root / name, meta, data or datasets())

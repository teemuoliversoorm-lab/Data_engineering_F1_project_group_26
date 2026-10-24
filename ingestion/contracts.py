"""Validate source batches before any database mutation; no network dependencies."""

import math
from datetime import date

DATASETS = ("laps", "session_results")
REQUIRED_COLUMNS = {
    "laps": {"DriverNumber", "LapNumber", "LapTime", "Position", "TrackStatus", "Time", "Driver", "Team"},
    "session_results": {"DriverNumber", "DriverId", "TeamId", "FullName", "Abbreviation", "Position", "Q1", "Q2", "Q3", "GridPosition", "Points", "Status"},
}


class DataQualityError(ValueError):
    """The source data cannot safely replace an existing batch."""


def positive_integer(value, name):
    if isinstance(value, bool):
        raise DataQualityError(f"{name} must be a positive integer")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DataQualityError(f"{name} must be a positive integer") from exc
    if not math.isfinite(number) or not number.is_integer() or number <= 0:
        raise DataQualityError(f"{name} must be a positive integer")
    return int(number)


def driver_number(row):
    return str(positive_integer(row.get("DriverNumber"), "DriverNumber"))


def validate_parameters(season, rounds, sessions, cutoff):
    season = positive_integer(season, "season")
    if not 2023 <= season <= date.today().year + 1:
        raise ValueError("OpenF1 historical coverage starts in 2023; season is out of range")
    rounds = sorted({positive_integer(r, "round") for r in rounds})
    if not rounds or any(r > 30 for r in rounds):
        raise ValueError("Specify at least one round between 1 and 30")
    sessions = list(dict.fromkeys(sessions))
    if not sessions or set(sessions) - {"Q", "R"}:
        raise ValueError("sessions must contain Q and/or R")
    date.fromisoformat(cutoff)
    return {"season": season, "rounds": rounds, "sessions": sessions, "cutoff": cutoff}


def validate_dataset(dataset, rows):
    if dataset not in DATASETS:
        raise DataQualityError(f"Unknown dataset: {dataset}")
    if not isinstance(rows, list) or not rows:
        raise DataQualityError(f"{dataset}: empty source cannot replace valid data")
    keys = set()
    ids = set()
    for row in rows:
        if not isinstance(row, dict) or REQUIRED_COLUMNS[dataset] - row.keys():
            raise DataQualityError(f"{dataset}: missing required source columns")
        number = driver_number(row)
        if dataset == "laps":
            key = (number, positive_integer(row.get("LapNumber"), "LapNumber"))
        else:
            key = number
            for field in ("DriverId", "TeamId"):
                if not isinstance(row.get(field), str) or not row[field].strip():
                    raise DataQualityError(f"session_results: missing {field}")
            if row["DriverId"] in ids:
                raise DataQualityError("session_results: duplicate DriverId")
            ids.add(row["DriverId"])
        if key in keys:
            raise DataQualityError(f"{dataset}: duplicate business key {key}")
        keys.add(key)
        for field in ("Position", "GridPosition"):
            value = row.get(field)
            if value is not None:
                lower = 0 if field == "GridPosition" else 1
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not float(value).is_integer() or not lower <= value <= 24:
                    raise DataQualityError(f"{dataset}: {field} outside {lower}..24")
    return len(keys)


def validate_pair(laps, results, allow_empty_laps=False):
    if not (allow_empty_laps and isinstance(laps, list) and not laps):
        validate_dataset("laps", laps)
    validate_dataset("session_results", results)
    known = {driver_number(row) for row in results}
    missing = {driver_number(row) for row in laps} - known
    if missing:
        raise DataQualityError(f"Lap drivers absent from session results: {sorted(missing)}")

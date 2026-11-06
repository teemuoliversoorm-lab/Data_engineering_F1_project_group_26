"""Data-quality checks run before any row is written to bronze.

Project 2 asks for at least one data-quality check in the pipeline; this
module is deliberately the single place where "is this batch good enough to
load?" is decided, so it's easy to find and easy to extend.
"""

from datetime import date

DATASETS = ("laps", "session_results")

# Columns every row of a dataset must have before we trust it enough to load.
REQUIRED_COLUMNS = {
    "laps": {"DriverNumber", "LapNumber", "LapTime", "Position"},
    "session_results": {"DriverNumber", "DriverId", "TeamId", "Position", "GridPosition", "Points"},
}


class DataQualityError(ValueError):
    """Raised when a batch of source data fails validation and must not be loaded."""


def validate_parameters(season, rounds, sessions, cutoff):
    """Check the DAG's own run parameters before spending an API call on them."""
    if not isinstance(season, int) or not 2023 <= season <= date.today().year + 1:
        raise ValueError("OpenF1 historical coverage starts in 2023; season is out of range")
    rounds = sorted({int(r) for r in rounds})
    if not rounds or any(r < 1 or r > 30 for r in rounds):
        raise ValueError("Specify at least one round between 1 and 30")
    sessions = list(dict.fromkeys(sessions))
    if not sessions or set(sessions) - {"Q", "R"}:
        raise ValueError("sessions must contain Q and/or R")
    date.fromisoformat(cutoff)  # raises ValueError if not an actual YYYY-MM-DD date
    return {"season": season, "rounds": rounds, "sessions": sessions, "cutoff": cutoff}


def check_dataset(dataset, rows):
    """The pipeline's required data-quality check.

    Rejects a batch that is empty, missing required fields, has duplicate
    business keys, or has grid/finishing positions outside the physically
    possible 1..24 range. Returns the number of rows accepted, or raises
    DataQualityError -- a rejected batch is never loaded (see 01_ingestion/
    load_bronze.py), so a bad source response can't silently corrupt bronze.
    """
    if dataset not in DATASETS:
        raise DataQualityError(f"Unknown dataset: {dataset}")
    if not rows:
        raise DataQualityError(f"{dataset}: empty source cannot replace valid data")

    seen_keys = set()
    for row in rows:
        if REQUIRED_COLUMNS[dataset] - row.keys():
            raise DataQualityError(f"{dataset}: row missing required columns")

        key = (row["DriverNumber"], row["LapNumber"]) if dataset == "laps" else row["DriverNumber"]
        if key in seen_keys:
            raise DataQualityError(f"{dataset}: duplicate key {key}")
        seen_keys.add(key)

        for field in ("Position", "GridPosition"):
            value = row.get(field)
            if value is not None and not (0 <= value <= 24):
                raise DataQualityError(f"{dataset}: {field}={value} outside the 0..24 grid")

    return len(seen_keys)

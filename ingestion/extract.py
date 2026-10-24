"""Extract completed historical OpenF1 sessions into replayable source bundles."""

import hashlib
import json
import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.contracts import DataQualityError, validate_pair, validate_parameters
from ingestion.openf1 import ADAPTER_VERSION, OpenF1Client, adapt, schedule, timestamp


def json_bytes(value):
    return json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")).encode()


def write_bundle(directory, metadata, datasets, raw_responses=None):
    """Immutable per-attempt folder; manifest is published last."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    manifest = {"version": 1, "metadata": dict(metadata), "files": {}}
    if raw_responses is not None:
        content = json_bytes(raw_responses)
        (directory / "openf1_raw.json").write_bytes(content)
        manifest["raw_file"] = {"filename": "openf1_raw.json",
                                "sha256": hashlib.sha256(content).hexdigest()}
        manifest["metadata"]["raw_sha256"] = manifest["raw_file"]["sha256"]
    for dataset, rows in datasets.items():
        content = json_bytes(rows)
        filename = f"{dataset}.json"
        (directory / filename).write_bytes(content)
        manifest["files"][dataset] = {
            "filename": filename, "sha256": hashlib.sha256(content).hexdigest(),
            "rows": len(rows), "columns": sorted({key for row in rows for key in row}),
        }
    path = directory / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return str(path.resolve())


def reusable_bundle(root, season, round_number, session_type, cutoff):
    from ingestion.load_bronze import read_bundle, read_raw_responses
    directory = Path(root) / str(season) / str(round_number) / session_type
    candidates = list(directory.glob("*/manifest.json"))
    if not candidates:
        return None
    # Validate the newest attempt; never silently fall back from a corrupt correction.
    path = max(candidates, key=lambda candidate: (candidate.stat().st_mtime_ns, str(candidate)))
    manifest, _ = read_bundle(path)
    meta = manifest["metadata"]
    if (meta.get("provider") != "OpenF1"
            or (meta["season"], meta["round_number"], meta["session_type"])
            != (season, round_number, session_type)):
        raise DataQualityError("Landed bundle does not match the requested OpenF1 session")
    raw = read_raw_responses(path, manifest)
    source_sessions = [row for response in raw.values() for row in response["records"]
                       if row.get("session_key") == meta["session_key"] and "date_end" in row
                       and "session_name" in row]
    if len(source_sessions) != 1:
        raise DataQualityError("Landed bundle has no unambiguous session end time")
    end_date = timestamp(source_sessions[0]["date_end"]).date()
    if end_date >= date.fromisoformat(cutoff) or end_date >= datetime.now(timezone.utc).date():
        raise DataQualityError("Landed session not before cutoff/completed day")
    return str(path.resolve())


def extract_session(season, round_number, session_type, cutoff, data_dir=None, cache_dir=None,
                    client=None, reuse_landed=False, allow_missing_laps=False):
    config = validate_parameters(season, [round_number], [session_type], cutoff)
    season, round_number = config["season"], config["rounds"][0]
    root = Path(data_dir or os.environ.get("F1_DATA_DIR", "tmp/landing"))
    if reuse_landed:
        existing = reusable_bundle(root, season, round_number, session_type, cutoff)
        if existing:
            from ingestion.load_bronze import read_bundle
            saved, _ = read_bundle(existing)
            if saved["metadata"].get("dataset_status", {}).get("laps") == "unavailable" and not allow_missing_laps:
                raise DataQualityError("Saved bundle has missing laps; enable allow_missing_laps or refresh the source")
            print(f"Reusing validated historical bundle: {existing}")
            return existing
    client = client or OpenF1Client(
        state_dir=cache_dir or os.environ.get("F1_CACHE_DIR", "tmp/api-state"))
    sessions, races = schedule(client, season)
    if round_number > len(races):
        raise DataQualityError(f"OpenF1 has no race calendar entry for {season} round {round_number}")
    race = races[round_number - 1]
    selected = [row for row in sessions
                if row.get("meeting_key") == race["meeting_key"]
                and row.get("session_name") == {"Q": "Qualifying", "R": "Race"}[session_type]
                and not row.get("is_cancelled", False)]
    if len(selected) != 1:
        raise DataQualityError(f"OpenF1 requested session missing or ambiguous: {season}/{round_number}/{session_type}")
    session = selected[0]
    end_date = timestamp(session["date_end"]).date()
    if end_date >= date.fromisoformat(cutoff) or end_date >= datetime.now(timezone.utc).date():
        raise DataQualityError(f"{season}/{round_number}/{session_type}: session not before cutoff/completed day")
    meetings = client.get("meetings", meeting_key=race["meeting_key"])
    if len(meetings) != 1:
        raise DataQualityError("OpenF1 meeting metadata missing or ambiguous")
    meeting = meetings[0]
    key = session["session_key"]
    raw_laps = client.get("laps", session_key=key, allow_empty=allow_missing_laps)
    raw_results = client.get("session_result", session_key=key)
    drivers = client.get("drivers", session_key=key)
    positions = client.get("position", session_key=key, allow_empty=True) if session_type == "R" else []
    grid = []
    grid_key = None
    if session_type == "R":
        grid = client.get("starting_grid", session_key=key, allow_empty=True)
        grid_key = key if grid else None
        # Some OpenF1 historical grids are attached to the qualifying session.
        if not grid:
            qualifying = [row for row in sessions if row.get("meeting_key") == race["meeting_key"]
                          and row.get("session_name") == "Qualifying"
                          and not row.get("is_cancelled", False)]
            if len(qualifying) == 1:
                alternate = qualifying[0]["session_key"]
                grid = client.get("starting_grid", session_key=alternate, allow_empty=True)
                grid_key = alternate if grid else None
    datasets = adapt(session_type, raw_laps, raw_results, drivers, grid, positions)
    validate_pair(datasets["laps"], datasets["session_results"], allow_empty_laps=allow_missing_laps)
    missing_classifications = [int(row["DriverNumber"]) for row in datasets["session_results"]
                               if not row["ClassificationAvailable"]]
    metadata = {
        "season": season, "round_number": round_number, "session_type": session_type,
        "cutoff": cutoff, "extracted_at": datetime.now(timezone.utc).isoformat(),
        "provider": "OpenF1", "adapter_version": ADAPTER_VERSION,
        "dataset_status": {"laps": "available" if raw_laps else "unavailable",
                           "session_results": "partial" if missing_classifications else "available"},
        "source_classification_rows": len(raw_results),
        "participants_without_classification": missing_classifications,
        "session_key": key, "meeting_key": race["meeting_key"], "grid_session_key": grid_key,
        "round_mapping": "chronological_non_cancelled_main_race_sessions",
        "event_name": meeting["meeting_name"], "country": meeting["country_name"],
        "circuit_name": meeting["circuit_short_name"],
        "session_date": timestamp(session["date_start"]).date().isoformat(),
        "event_date": timestamp(race["date_start"]).date().isoformat(),
    }
    return write_bundle(root / str(season) / str(round_number) / session_type / uuid.uuid4().hex,
                        metadata, datasets, raw_responses=client.responses)

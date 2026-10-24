"""Validate landed bytes and atomically replace both datasets for one session."""

import hashlib
import json
from pathlib import Path

from ingestion.contracts import DATASETS, DataQualityError, driver_number, positive_integer, validate_pair, validate_parameters


def read_raw_responses(manifest_path, manifest):
    spec = manifest.get("raw_file")
    if spec is None:
        if manifest["metadata"].get("provider") == "OpenF1":
            raise DataQualityError("OpenF1 bundle requires raw response provenance")
        return {}
    if spec.get("filename") != "openf1_raw.json":
        raise DataQualityError("Unexpected raw source filename")
    content = (Path(manifest_path).parent / spec["filename"]).read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if digest != spec["sha256"] or manifest["metadata"].get("raw_sha256") != digest:
        raise DataQualityError("OpenF1 raw checksum mismatch")
    responses = json.loads(content)
    if not isinstance(responses, dict) or not responses:
        raise DataQualityError("OpenF1 raw responses must be a nonempty object")
    for query, response in responses.items():
        if (not isinstance(query, str) or not isinstance(response, dict)
                or not isinstance(response.get("url"), str)
                or not response["url"].startswith("https://api.openf1.org/v1/")
                or not isinstance(response.get("records"), list)
                or any(not isinstance(row, dict) for row in response["records"])):
            raise DataQualityError("Invalid OpenF1 raw response envelope")
    return responses


def read_bundle(manifest_path):
    path = Path(manifest_path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("version") != 1 or set(manifest.get("files", {})) != set(DATASETS):
        raise DataQualityError("Manifest must contain both datasets and version 1")
    metadata = manifest["metadata"]
    config = validate_parameters(metadata["season"], [metadata["round_number"]], [metadata["session_type"]], metadata["cutoff"])
    metadata["season"] = config["season"]
    metadata["round_number"] = config["rounds"][0]
    datasets = {}
    for dataset in DATASETS:
        spec = manifest["files"][dataset]
        if spec["filename"] != f"{dataset}.json":
            raise DataQualityError("Unexpected source filename")
        content = (path.parent / spec["filename"]).read_bytes()
        if hashlib.sha256(content).hexdigest() != spec["sha256"]:
            raise DataQualityError(f"{dataset}: checksum mismatch")
        datasets[dataset] = json.loads(content)
        if len(datasets[dataset]) != spec["rows"]:
            raise DataQualityError(f"{dataset}: row count mismatch")
    missing_laps = metadata.get("dataset_status", {}).get("laps") == "unavailable"
    if missing_laps and datasets["laps"]:
        raise DataQualityError("Unavailable laps must contain no fabricated records")
    validate_pair(datasets["laps"], datasets["session_results"], allow_empty_laps=missing_laps)
    unclassified = [row for row in datasets["session_results"] if row.get("ClassificationAvailable") is False]
    if unclassified and metadata.get("dataset_status", {}).get("session_results") != "partial":
        raise DataQualityError("Participants without classifications require an explicit partial-source flag")
    read_raw_responses(path, manifest)
    return manifest, datasets


def initialize_database(connection):
    ddl = Path(__file__).resolve().parents[1] / "SQL" / "bronze_setup.sql"
    with connection.transaction():
        connection.execute(ddl.read_text(encoding="utf-8"))


def load_bundle(manifest_path, connection=None):
    import psycopg
    from psycopg.types.json import Jsonb

    manifest, datasets = read_bundle(manifest_path)
    raw_responses = read_raw_responses(manifest_path, manifest)
    metadata = manifest["metadata"]
    batch = (metadata["season"], metadata["round_number"], metadata["session_type"])
    own_connection = connection is None
    connection = connection or psycopg.connect("")
    try:
        with connection.transaction():
            lock_id = int.from_bytes(hashlib.sha256(repr(batch).encode()).digest()[:8], "big", signed=True)
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
            missing_laps = metadata.get("dataset_status", {}).get("laps") == "unavailable"
            if missing_laps and connection.execute(
                "SELECT EXISTS (SELECT 1 FROM bronze.laps WHERE season=%s AND round_number=%s AND session_type=%s)", batch
            ).fetchone()[0]:
                raise DataQualityError("Missing source laps cannot replace previously loaded lap data")
            def stable_metadata(value):
                return {k: v for k, v in value.items() if k not in ("extracted_at", "cutoff")}
            existing = {
                name: (digest, stable_metadata(meta))
                for name, digest, meta in connection.execute(
                    "SELECT dataset, content_sha256, metadata FROM bronze.load_audit WHERE season=%s AND round_number=%s AND session_type=%s", batch
                ).fetchall()
            }
            expected = {name: (manifest["files"][name]["sha256"], stable_metadata(metadata)) for name in DATASETS}
            if existing == expected:
                return {"status": "unchanged", "batch": batch}
            connection.execute("DELETE FROM bronze.laps WHERE season=%s AND round_number=%s AND session_type=%s", batch)
            connection.execute("DELETE FROM bronze.session_results WHERE season=%s AND round_number=%s AND session_type=%s", batch)
            connection.execute("DELETE FROM bronze.source_responses WHERE season=%s AND round_number=%s AND session_type=%s", batch)
            with connection.cursor() as cursor:
                for query, response in sorted(raw_responses.items()):
                    content = json.dumps(response["records"], sort_keys=True, allow_nan=False, separators=(",", ":")).encode()
                    cursor.execute(
                        """INSERT INTO bronze.source_responses
                        (season,round_number,session_type,source_query,source_url,content_sha256,source_records,metadata)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (*batch, query, response["url"], hashlib.sha256(content).hexdigest(),
                         Jsonb(response["records"]), Jsonb(metadata)),
                    )
                cursor.executemany(
                    "INSERT INTO bronze.session_results (season,round_number,session_type,driver_number,driver_id,team_id,source_record,metadata) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    [(*batch, driver_number(row), row["DriverId"], row["TeamId"], Jsonb(row), Jsonb(metadata)) for row in datasets["session_results"]],
                )
                cursor.executemany(
                    "INSERT INTO bronze.laps (season,round_number,session_type,driver_number,lap_number,source_record,metadata) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    [(*batch, driver_number(row), positive_integer(row["LapNumber"], "LapNumber"), Jsonb(row), Jsonb(metadata)) for row in datasets["laps"]],
                )
                for dataset in DATASETS:
                    spec = manifest["files"][dataset]
                    cursor.execute(
                        """INSERT INTO bronze.load_audit (season,round_number,session_type,dataset,content_sha256,row_count,source_path,metadata)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (season,round_number,session_type,dataset) DO UPDATE SET
                        content_sha256=excluded.content_sha256, row_count=excluded.row_count,
                        source_path=excluded.source_path, metadata=excluded.metadata, loaded_at=now()""",
                        (*batch, dataset, spec["sha256"], spec["rows"], str(Path(manifest_path).resolve()), Jsonb(metadata)),
                    )
        statuses = metadata.get("dataset_status", {})
        partial = [name for name, status in statuses.items() if status == "partial"]
        result = {"status": "loaded_with_gaps" if missing_laps or partial else "loaded", "batch": batch,
                  "rows": {k: len(v) for k, v in datasets.items()}}
        if missing_laps:
            result["missing_datasets"] = ["laps"]
        if partial:
            result["partial_datasets"] = partial
            result["participants_without_classification"] = metadata.get("participants_without_classification", [])
        return result
    finally:
        if own_connection:
            connection.close()

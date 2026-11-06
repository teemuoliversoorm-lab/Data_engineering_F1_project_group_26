"""Load one validated session batch into bronze.

Idempotency strategy: delete this (season, round_number, session_type) batch,
then insert the freshly extracted rows, all inside one transaction. Running
the same input again produces exactly the same rows -- no duplicates, and no
separate "has this already been loaded?" bookkeeping is needed.
"""

from pathlib import Path


def initialize_database(connection):
    # sql/bronze_setup.sql lives next to this file, both on disk (01_ingestion/sql/)
    # and inside the container (ingestion/sql/) -- see docker/airflow.Dockerfile.
    ddl = Path(__file__).resolve().parent / "sql" / "bronze_setup.sql"
    with connection.transaction():
        connection.execute(ddl.read_text(encoding="utf-8"))


def load_session(payload, connection=None):
    """payload is extract_session()'s return value, already checked with
    ingestion.quality.check_dataset for both datasets."""
    import psycopg
    from psycopg.types.json import Jsonb

    metadata = payload["metadata"]
    batch = (metadata["season"], metadata["round_number"], metadata["session_type"])
    laps, results = payload["laps"], payload["session_results"]

    # A lap must belong to a driver we have a result row for, otherwise the
    # foreign key below would reject it -- fail clearly instead of partially loading.
    known_drivers = {row["DriverNumber"] for row in results}
    missing = {row["DriverNumber"] for row in laps} - known_drivers
    if missing:
        raise ValueError(f"Lap drivers absent from session results: {sorted(missing)}")

    own_connection = connection is None
    connection = connection or psycopg.connect("")
    try:
        with connection.transaction():
            connection.execute(
                "DELETE FROM bronze.laps WHERE season=%s AND round_number=%s AND session_type=%s", batch)
            connection.execute(
                "DELETE FROM bronze.session_results WHERE season=%s AND round_number=%s AND session_type=%s", batch)
            with connection.cursor() as cursor:
                cursor.executemany(
                    """INSERT INTO bronze.session_results
                    (season, round_number, session_type, driver_number, driver_id, team_id, source_record, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    [(*batch, row["DriverNumber"], row["DriverId"], row["TeamId"], Jsonb(row), Jsonb(metadata))
                     for row in results],
                )
                cursor.executemany(
                    """INSERT INTO bronze.laps
                    (season, round_number, session_type, driver_number, lap_number, source_record, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    [(*batch, row["DriverNumber"], row["LapNumber"], Jsonb(row), Jsonb(metadata))
                     for row in laps],
                )
        return {"status": "loaded", "batch": batch, "rows": {"laps": len(laps), "session_results": len(results)}}
    finally:
        if own_connection:
            connection.close()

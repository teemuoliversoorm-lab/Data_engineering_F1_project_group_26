"""Extract one OpenF1 session (laps + session_results), ready for loading
into bronze.

Returns the rows directly instead of writing files to disk -- the Airflow
DAG passes the result from task to task through XCom (see 00_dags/f1_ingestion.py).
"""

from datetime import date, datetime, timezone

from ingestion.openf1 import OpenF1Client, adapt, parse_timestamp, schedule
from ingestion.quality import DataQualityError, validate_parameters

SESSION_NAMES = {"Q": "Qualifying", "R": "Race"}


def extract_session(season, round_number, session_type, cutoff, client=None):
    config = validate_parameters(season, [round_number], [session_type], cutoff)
    season, round_number = config["season"], config["rounds"][0]
    client = client or OpenF1Client()

    sessions, races = schedule(client, season)
    if round_number > len(races):
        raise DataQualityError(f"OpenF1 has no race calendar entry for {season} round {round_number}")
    race = races[round_number - 1]

    matches = [row for row in sessions
               if row["meeting_key"] == race["meeting_key"]
               and row["session_name"] == SESSION_NAMES[session_type] and not row.get("is_cancelled")]
    if len(matches) != 1:
        raise DataQualityError(f"OpenF1 session missing or ambiguous: {season}/{round_number}/{session_type}")
    session = matches[0]

    end_date = parse_timestamp(session["date_end"]).date()
    if end_date >= date.fromisoformat(cutoff) or end_date >= datetime.now(timezone.utc).date():
        raise DataQualityError(f"{season}/{round_number}/{session_type}: session not before cutoff/completed day")

    meeting = client.get("meetings", meeting_key=race["meeting_key"])[0]
    key = session["session_key"]

    raw_laps = client.get("laps", session_key=key)
    raw_results = client.get("session_result", session_key=key)
    drivers = client.get("drivers", session_key=key)
    positions = client.get("position", session_key=key, allow_empty=True) if session_type == "R" else []

    grid = []
    if session_type == "R":
        grid = client.get("starting_grid", session_key=key, allow_empty=True)
        if not grid:
            # Some OpenF1 historical grids are attached to the qualifying session instead.
            qualifying = [row for row in sessions if row["meeting_key"] == race["meeting_key"]
                          and row["session_name"] == "Qualifying" and not row.get("is_cancelled")]
            if len(qualifying) == 1:
                grid = client.get("starting_grid", session_key=qualifying[0]["session_key"], allow_empty=True)

    datasets = adapt(session_type, raw_laps, raw_results, drivers, grid, positions)
    return {
        "metadata": {
            "season": season, "round_number": round_number, "session_type": session_type,
            "cutoff": cutoff, "provider": "OpenF1",
            "event_name": meeting["meeting_name"], "country": meeting["country_name"],
            "circuit_name": meeting["circuit_short_name"],
            "session_date": parse_timestamp(session["date_start"]).date().isoformat(),
            "event_date": parse_timestamp(race["date_start"]).date().isoformat(),
        },
        "laps": datasets["laps"],
        "session_results": datasets["session_results"],
    }

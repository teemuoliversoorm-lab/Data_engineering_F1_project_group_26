"""CLI entry points, used for manual debugging outside Airflow."""

import argparse
import json

from ingestion.extract import extract_session
from ingestion.load_bronze import initialize_database, load_session
from ingestion.quality import check_dataset


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init-db")
    commands.add_parser("schedule").add_argument("--season", type=int, required=True)
    ingest = commands.add_parser("ingest", help="extract, check and load one session")
    ingest.add_argument("--season", type=int, required=True)
    ingest.add_argument("--round", dest="round_number", type=int, required=True)
    ingest.add_argument("--session", dest="session_type", choices=["Q", "R"], required=True)
    ingest.add_argument("--cutoff", required=True, help="Exclusive UTC date, YYYY-MM-DD")
    args = vars(parser.parse_args())
    command = args.pop("command")

    if command == "schedule":
        from ingestion.openf1 import OpenF1Client, schedule
        _, races = schedule(OpenF1Client(), args["season"])
        result = [{"round": number, "session_key": row["session_key"], "country": row["country_name"],
                   "date_start": row["date_start"]} for number, row in enumerate(races, 1)]
    elif command == "init-db":
        import psycopg
        with psycopg.connect("") as connection:
            initialize_database(connection)
        result = {"status": "initialized"}
    else:  # ingest
        payload = extract_session(**args)
        check_dataset("laps", payload["laps"])
        check_dataset("session_results", payload["session_results"])
        result = load_session(payload)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

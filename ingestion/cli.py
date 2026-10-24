"""CLI entry points also used by the Airflow tasks."""

import argparse
import json

from ingestion.extract import extract_session
from ingestion.load_bronze import initialize_database, load_bundle, read_bundle


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init-db")
    commands.add_parser("schedule").add_argument("--season", type=int, required=True)
    extract = commands.add_parser("extract")
    extract.add_argument("--season", type=int, required=True)
    extract.add_argument("--round", dest="round_number", type=int, required=True)
    extract.add_argument("--session", dest="session_type", choices=["Q", "R"], required=True)
    extract.add_argument("--cutoff", required=True, help="Exclusive UTC date, YYYY-MM-DD")
    extract.add_argument("--reuse-landed", action="store_true", default=argparse.SUPPRESS)
    extract.add_argument("--allow-missing-laps", action="store_true", default=argparse.SUPPRESS)
    for command in ("check", "load"):
        commands.add_parser(command).add_argument("manifest")
    args = vars(parser.parse_args())
    command = args.pop("command")
    if command == "schedule":
        from ingestion.openf1 import OpenF1Client, schedule
        from ingestion.contracts import validate_parameters
        import os
        validate_parameters(args["season"], [1], ["R"], f"{args['season']}-01-01")
        _, races = schedule(OpenF1Client(state_dir=os.environ.get("F1_CACHE_DIR", "tmp/api-state")), args["season"])
        result = [{"round": number, "session_key": row["session_key"],
                   "meeting_key": row["meeting_key"], "country": row["country_name"],
                   "date_start": row["date_start"]} for number, row in enumerate(races, 1)]
    elif command == "init-db":
        import psycopg
        with psycopg.connect("") as connection:
            initialize_database(connection)
        result = {"status": "initialized"}
    elif command == "extract":
        result = {"manifest": extract_session(**args)}
    elif command == "check":
        manifest, datasets = read_bundle(args["manifest"])
        result = {"metadata": manifest["metadata"], "rows": {k: len(v) for k, v in datasets.items()}}
    else:
        result = load_bundle(args["manifest"])
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

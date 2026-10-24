"""Assert fixture gold values and print deterministic storage content hashes."""

import hashlib
import json
from pathlib import Path

import psycopg
from psycopg import sql

with psycopg.connect("") as connection:
    database = connection.execute("SELECT current_database()").fetchone()[0]
    if not database.startswith("f1_") or not database.endswith("_test"):
        raise ValueError("Run only against a separately prepared f1_*_test database")
    connection.execute(Path(__file__).with_suffix(".sql").read_text())
    result = {}
    for table in ("dimdriver", "dimteam", "dimevent", "dimdriverteamassignment", "factlap", "factdrivereventresult"):
        rows = connection.execute(sql.SQL("SELECT to_jsonb(t)::text FROM gold.{} t ORDER BY to_jsonb(t)::text").format(sql.Identifier(table))).fetchall()
        result[table] = {"rows": len(rows), "sha256": hashlib.sha256(json.dumps(rows).encode()).hexdigest()}
print(json.dumps(result, sort_keys=True))

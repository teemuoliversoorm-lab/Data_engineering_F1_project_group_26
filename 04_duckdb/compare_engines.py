import os
import statistics
import time
from decimal import Decimal
import duckdb
import psycopg2
from pathlib import Path
import csv


# Queries

POSTGRES_QUERIES = {
    "Q2 - team position change": """
        SELECT
            e.season,
            t.teamname,
            SUM(f.placesgainedlost) AS net_positions
        FROM gold.factdrivereventresult f
        JOIN gold.dimteam t USING (teamkey)
        JOIN gold.dimevent e USING (eventkey)
        WHERE f.placesgainedlost IS NOT NULL
        GROUP BY e.season, t.teamname
        ORDER BY e.season, net_positions DESC, t.teamname;
    """,

    "Q4 - pole retained lead after lap 1": """
        WITH pole_sitters AS (
            SELECT
                eventkey,
                driverkey
            FROM gold.factdrivereventresult
            WHERE qualifyingposition = 1
        )
        SELECT
            e.season,
            e.roundnumber,
            e.eventname,
            e.circuitname,
            d.fullname AS pole_driver
        FROM pole_sitters p
        JOIN gold.factlap l
            ON p.eventkey = l.eventkey
           AND p.driverkey = l.driverkey
        JOIN gold.dimevent e
            ON p.eventkey = e.eventkey
        JOIN gold.dimdriver d
            ON p.driverkey = d.driverkey
        WHERE l.sessiontype = 'Race'
          AND l.lapnumber = 1
          AND l.lapposition = 1
        ORDER BY e.season, e.roundnumber;
    """
}

# DuckDB contains the same Gold tables, but without the "gold." schema prefix.
DUCKDB_QUERIES = {
    "Q2 - team position change": """
        SELECT
            e.season,
            t.teamname,
            SUM(f.placesgainedlost) AS net_positions
        FROM factdrivereventresult f
        JOIN dimteam t USING (teamkey)
        JOIN dimevent e USING (eventkey)
        WHERE f.placesgainedlost IS NOT NULL
        GROUP BY e.season, t.teamname
        ORDER BY e.season, net_positions DESC, t.teamname;
    """,

    "Q4 - pole retained lead after lap 1": """
        WITH pole_sitters AS (
            SELECT
                eventkey,
                driverkey
            FROM factdrivereventresult
            WHERE qualifyingposition = 1
        )
        SELECT
            e.season,
            e.roundnumber,
            e.eventname,
            e.circuitname,
            d.fullname AS pole_driver
        FROM pole_sitters p
        JOIN factlap l
            ON p.eventkey = l.eventkey
           AND p.driverkey = l.driverkey
        JOIN dimevent e
            ON p.eventkey = e.eventkey
        JOIN dimdriver d
            ON p.driverkey = d.driverkey
        WHERE l.sessiontype = 'Race'
          AND l.lapnumber = 1
          AND l.lapposition = 1
        ORDER BY e.season, e.roundnumber;
    """
}

# Connections

pg = psycopg2.connect(
    host="host.docker.internal",
    port=os.getenv("POSTGRES_PORT", "5433"),
    dbname=os.getenv("POSTGRES_DB", "f1_warehouse"),
    user=os.getenv("POSTGRES_USER", "f1"),
    password=os.environ["POSTGRES_PASSWORD"],
)

duck = duckdb.connect("/data/f1_analytics.duckdb")

# Helpers

def postgres_query(sql):
    with pg.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def duckdb_query(sql):
    return duck.execute(sql).fetchall()


def normalise(rows):
    """Make Decimal/float differences irrelevant when comparing results."""
    cleaned = []

    for row in rows:
        cleaned.append(
            tuple(
                float(value) if isinstance(value, Decimal) else value
                for value in row
            )
        )

    return cleaned


def median_runtime(query_function, sql, repetitions=5):
    # warm-up, so the data is already cached
    query_function(sql)

    timings = []

    for _ in range(repetitions):
        start = time.perf_counter()
        query_function(sql)
        end = time.perf_counter()

        timings.append((end - start) * 1000)

    return statistics.median(timings)


def postgres_explain(sql):
    with pg.cursor() as cur:
        cur.execute(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql
        )

        plan = cur.fetchone()[0][0]

    return (
        plan.get("Planning Time"),
        plan.get("Execution Time"),
    )


def duckdb_explain(sql):
    result = duck.execute("EXPLAIN ANALYZE " + sql).fetchall()

    return "\n".join(
        str(row[1] if len(row) > 1 else row[0])
        for row in result
    )

# Comparison

results = []

for name in POSTGRES_QUERIES:
    pg_sql = POSTGRES_QUERIES[name]
    duck_sql = DUCKDB_QUERIES[name]

    pg_result = postgres_query(pg_sql)
    duck_result = duckdb_query(duck_sql)

    matches = normalise(pg_result) == normalise(duck_result)

    pg_median = median_runtime(postgres_query, pg_sql)
    duck_median = median_runtime(duckdb_query, duck_sql)

    pg_planning, pg_execution = postgres_explain(pg_sql)

    print("\n" + "=" * 80)
    print(name)
    print("=" * 80)

    print(f"Results match: {matches}")
    print(f"Rows returned: {len(pg_result)}")

    print("\nPostgreSQL EXPLAIN ANALYZE:")
    print(f"Planning Time:  {pg_planning:.3f} ms")
    print(f"Execution Time: {pg_execution:.3f} ms")

    print("\nDuckDB EXPLAIN ANALYZE:")
    print(duckdb_explain(duck_sql))

    results.append(
        (
            name,
            matches,
            len(pg_result),
            pg_median,
            duck_median,
            pg_execution,
        )
    )


# -----------------------------
# Markdown summary table
# -----------------------------

print("\n\nCOMPARISON TABLE")
print()
print(
    "| Query | Same result? | Result rows | PostgreSQL median (ms) | "
    "DuckDB median (ms) | PostgreSQL EXPLAIN execution (ms) |"
)
print(
    "|---|---:|---:|---:|---:|---:|"
)

for (
    name,
    matches,
    rows,
    pg_median,
    duck_median,
    pg_execution,
) in results:
    print(
        f"| {name} | {'Yes' if matches else 'NO'} | "
        f"{rows} | {pg_median:.3f} | {duck_median:.3f} | "
        f"{pg_execution:.3f} |"
    )


duck.close()
pg.close()

# Prettier results

output_dir = Path("/data/results")
output_dir.mkdir(parents=True, exist_ok=True)

# Markdown
md_path = output_dir / "comparison.md"

with md_path.open("w", encoding="utf-8") as f:
    f.write("# PostgreSQL vs DuckDB\n\n")

    f.write(
        "| Query | Same result? | Result rows | PostgreSQL median (ms) | "
        "DuckDB median (ms) | PostgreSQL EXPLAIN execution (ms) |\n"
    )

    f.write(
        "|---|---:|---:|---:|---:|---:|\n"
    )

    for (
        name,
        matches,
        rows,
        pg_median,
        duck_median,
        pg_execution,
    ) in results:
        f.write(
            f"| {name} | {'Yes' if matches else 'No'} | "
            f"{rows} | {pg_median:.3f} | {duck_median:.3f} | "
            f"{pg_execution:.3f} |\n"
        )

print(f"\nSaved results to {output_dir}")
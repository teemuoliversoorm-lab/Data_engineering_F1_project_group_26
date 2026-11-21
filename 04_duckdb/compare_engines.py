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
            t.TeamName,
            SUM(r.PlacesGainedLost) AS net_positions
        FROM gold.FactDriverEventResult r
        JOIN gold.DimTeam t
        ON r.TeamKey = t.TeamKey
        GROUP BY t.TeamName
        ORDER BY net_positions DESC, t.TeamName ASC;
    """,

    "Q4 - pole retained lead after lap 1": """
        SELECT
            e.Season,
            e.EventName,
            d.DriverId
        FROM gold.FactLap l
        JOIN gold.DimEvent e
        ON l.EventKey = e.EventKey
        JOIN gold.DimDriver d
        ON l.DriverKey = d.DriverKey
        JOIN gold.FactDriverEventResult r
        ON l.EventKey = r.EventKey
        AND l.DriverKey = r.DriverKey
        WHERE r.QualifyingPosition = 1
        AND l.LapNumber = 1
        AND l.LapPosition = 1
        AND l.SessionType = 'Race'
        ORDER BY e.Season, e.EventName, d.DriverId;
    """
}

# DuckDB contains the same Gold tables, but without the "gold." schema prefix.
DUCKDB_QUERIES = {
    "Q2 - team position change": """
        SELECT
            t.TeamName,
            SUM(r.PlacesGainedLost) AS net_positions
        FROM FactDriverEventResult r
        JOIN DimTeam t
        ON r.TeamKey = t.TeamKey
        GROUP BY t.TeamName
        ORDER BY net_positions DESC, t.TeamName ASC;
    """,

    "Q4 - pole retained lead after lap 1": """
        SELECT
            e.Season,
            e.EventName,
            d.DriverId
        FROM FactLap l
        JOIN DimEvent e
        ON l.EventKey = e.EventKey
        JOIN DimDriver d
        ON l.DriverKey = d.DriverKey
        JOIN FactDriverEventResult r
        ON l.EventKey = r.EventKey
        AND l.DriverKey = r.DriverKey
        WHERE r.QualifyingPosition = 1
        AND l.LapNumber = 1
        AND l.LapPosition = 1
        AND l.SessionType = 'Race'
        ORDER BY e.Season, e.EventName, d.DriverId;
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
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return columns, rows


def duckdb_query(sql):
    result = duck.execute(sql)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    return columns, rows

def normalise(rows):
    cleaned = []

    for row in rows:
        cleaned.append(
            tuple(
                float(value) if isinstance(value, Decimal) else value
                for value in row
            )
        )

    return cleaned


def canonicalise(rows):
    return sorted(
        normalise(rows),
        key=lambda row: tuple(str(value) for value in row)
    )

def median_runtime(query_function, sql, repetitions=5):
    # warm-up
    query_function(sql)

    timings = []

    for _ in range(repetitions):
        start = time.perf_counter()
        query_function(sql)
        end = time.perf_counter()

        timings.append((end - start) * 1000)

    return statistics.median(timings)

def markdown_table(columns, rows):
    lines = []

    lines.append(
        "| " + " | ".join(str(column) for column in columns) + " |"
    )

    lines.append(
        "| " + " | ".join("---" for _ in columns) + " |"
    )

    for row in rows:
        lines.append(
            "| " + " | ".join(
                "" if value is None else str(value)
                for value in row
            ) + " |"
        )

    return "\n".join(lines)

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
query_outputs = {}

for name in POSTGRES_QUERIES:
    pg_sql = POSTGRES_QUERIES[name]
    duck_sql = DUCKDB_QUERIES[name]

    pg_columns, pg_result = postgres_query(pg_sql)
    duck_columns, duck_result = duckdb_query(duck_sql)

    matches = canonicalise(pg_result) == canonicalise(duck_result)
    
    pg_median = median_runtime(postgres_query, pg_sql)
    duck_median = median_runtime(duckdb_query, duck_sql)

    pg_planning, pg_execution = postgres_explain(pg_sql)

    print("\n" + "=" * 80)
    print(name)
    print("=" * 80)

    print(f"Results match: {matches}")
    print(f"PostgreSQL rows: {len(pg_result)}")
    print(f"DuckDB rows: {len(duck_result)}")

    print("\nPOSTGRESQL RESULT:")
    print(markdown_table(pg_columns, pg_result))

    print("\nDUCKDB RESULT:")
    print(markdown_table(duck_columns, duck_result))

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

    # Store actual results so we can export them later
    query_outputs[name] = {
        "columns": pg_columns,
        "rows": pg_result,
        "matches": matches,
    }

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

# Save actual business-query results
for name, output in query_outputs.items():

    safe_name = (
        name.lower()
        .replace(" ", "_")
        .replace("-", "")
    )

    columns = output["columns"]
    rows = output["rows"]

    # Markdown result
    result_md = output_dir / f"{safe_name}_results.md"

    with result_md.open("w", encoding="utf-8") as f:
        f.write(f"# {name}\n\n")
        f.write(
            f"PostgreSQL and DuckDB results match: "
            f"{'Yes' if output['matches'] else 'No'}\n\n"
        )
        f.write(markdown_table(columns, rows))
        f.write("\n")

    # CSV result
    result_csv = output_dir / f"{safe_name}_results.csv"

    with result_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

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
import os
import statistics
import time
from decimal import Decimal
import duckdb
import psycopg2
from pathlib import Path

# Queries

QUERY_FILES = {
    "Q2 - team position change":
        "/queries/query_02_team_position_change.sql",

    "Q4 - P1 retained lead after lap 1":
        "/queries/query_04_lap1_leader.sql",
}


def load_query(path):
    return Path(path).read_text(encoding="utf-8").strip()


POSTGRES_QUERIES = {
    name: load_query(path)
    for name, path in QUERY_FILES.items()
}


# DuckDB contains the Gold tables without the PostgreSQL "gold." schema.
DUCKDB_QUERIES = {
    name: sql.replace("gold.", "")
    for name, sql in POSTGRES_QUERIES.items()
}

# Connections

pg = psycopg2.connect(
    host=os.getenv("PGHOST", "warehouse"),
    port=os.getenv("PGPORT", "5432"),
    dbname=os.getenv("PGDATABASE", "f1_warehouse"),
    user=os.getenv("PGUSER", "f1"),
    password=os.environ["PGPASSWORD"],
    options="-c search_path=gold,public",
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
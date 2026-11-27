import os

import duckdb
import pandas as pd
import psycopg


TABLES = [
    "dimevent",
    "dimdriver",
    "dimteam",
    "factdrivereventresult",
    "factlap",
]

pg = psycopg.connect(
    host=os.environ.get("PGHOST", "warehouse"),
    port=os.environ.get("PGPORT", "5432"),
    dbname=os.environ.get("PGDATABASE", "f1_warehouse"),
    user=os.environ.get("PGUSER", "f1"),
    password=os.environ["PGPASSWORD"],
)

duck = duckdb.connect("/data/f1_analytics.duckdb")

for table in TABLES:
    df = pd.read_sql_query(f"SELECT * FROM gold.{table}", pg)

    duck.register("source_df", df)
    duck.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM source_df")
    duck.unregister("source_df")

    print(f"{table}: {len(df)} rows copied")

duck.close()
pg.close()

print("DuckDB refresh complete.")

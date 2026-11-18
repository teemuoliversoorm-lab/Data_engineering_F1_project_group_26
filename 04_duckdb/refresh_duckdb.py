import os

import duckdb
import pandas as pd
import psycopg2


TABLES = [
    "dimevent",
    "dimdriver",
    "dimteam",
    "factdrivereventresult",
    "factlap",
]


pg = psycopg2.connect(
    host=os.getenv("PGHOST", "host.docker.internal"),
    port=os.getenv("PGPORT", "5433"),
    dbname=os.getenv("POSTGRES_DB", "f1_warehouse"),
    user=os.getenv("POSTGRES_USER", "f1"),
    password=os.environ["POSTGRES_PASSWORD"],
)

duck = duckdb.connect("/data/f1_analytics.duckdb")

for table in TABLES:
    df = pd.read_sql_query(
        f"SELECT * FROM gold.{table}",
        pg
    )

    duck.register("source_df", df)

    duck.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM source_df
        """
    )

    duck.unregister("source_df")

    print(f"{table}: {len(df)} rows copied")

duck.close()
pg.close()

print("DuckDB refresh complete.")
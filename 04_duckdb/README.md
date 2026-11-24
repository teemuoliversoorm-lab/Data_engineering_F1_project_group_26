Running the comparison between PostgreSQL and DuckDB after the main setup:

Ensure that both build_warehouse and refresh_duckdb complete successfully in the triggered f1_ingestion DAG.
Then run:

docker compose --env-file .env run --rm duckdb-compare

If PostgreSQL Gold has been built already and you just want to refresh DuckDB without rerunning the entire DAG:

docker compose --env-file .env run --rm duckdb-compare python refresh_duckdb.py

DuckDB is used as an optional downstream analytical copy of the PostgreSQL Gold layer. After dbt builds Gold, Airflow refreshes the dimensional tables into a persistent DuckDB database stored in a Docker volume. DuckDB is a good fit for the analytical workload because its columnar storage is designed for reading selected columns across many rows and performing joins and aggregations. This matches Q2, which groups and sums position changes by season and team, and Q4, which joins lap, event, driver and race-result data to identify cases where the P1 starter retained the lead after lap 1. 

PostgreSQL remains the primary warehouse because it supports ingestion and the full Bronze–Silver–Gold transformation workflow, while DuckDB is refreshed in batches and used only for analytics. To validate DuckDB, queries Q2 and Q4 were executed in both engines. Each query was run once for warm-up and five additional times per engine, with median execution times reported. As can be seen in Table 1, in the current small dataset, PostgreSQL was faster for Q2 and Q4, although both engines returned identical results. The comparison therefore demonstrates DuckDB's suitability as a columnar analytical layer rather than claiming a general performance advantage. 

| Query | Same result? | PostgreSQL median (ms) | DuckDB median (ms) |
|---|---|---|---|
| b – Team position change | Yes | 0.754 | 4.009 |
| d – Pole retained lead after lap 1 | Yes | 1.086 | 8.631 |

*Table 1. Query execution time comparison.*


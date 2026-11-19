Running:
# docker build -t f1-duckdb .\04_duckdb
# docker run --rm --env-file .env -v "${PWD}\04_duckdb\data:/data" f1-duckdb python compare_engines.py

This runs b) and d) business question queries (Q2 and Q4) and outputs a comparison table between PostreSQL and DuckDB. 

Info for the report:
DuckDB was added as an optional analytical layer using a copy of the PostgreSQL Gold dimensional model. Refresh scripts replicate the Gold schema into native DuckDB tables, keeping DuckDB independent of the main pipeline.

DuckDB's columnar storage suits our analytical queries, which involve fact-table scans, dimension joins, grouping and aggregation. This is particularly relevant to queries **b** (team net position changes) and **d** (pole-position drivers retaining the lead after lap 1).

PostgreSQL remains the primary warehouse because it handles our small dataset, ingestion, dbt transformations and dimensional model effectively, and our team is familiar with it.

To validate DuckDB, queries **b** and **d** were executed in both engines, producing identical results.

| Query | Same result? | PostgreSQL median (ms) | DuckDB median (ms) |
|---|---|---|---|
| b – Team position change | Yes | 1.627 | 3.873 |
| d – Pole retained lead after lap 1 | Yes | 1.827 | 4.326 |

*Table 1. Query execution time comparison.*

Each query was run once for warm-up and five additional times per engine, with median execution times reported. PostgreSQL was faster for both queries, likely because the dataset is too small for DuckDB's columnar advantages to outweigh per-query overhead. These results should therefore not be treated as a general performance benchmark.

DuckDB introduces an additional Gold data copy that requires refreshing when PostgreSQL changes. However, it demonstrates that the same dimensional model can support a columnar analytical engine while producing identical results. DuckDB may become more beneficial as data volume and fact-table width increase, but PostgreSQL remains sufficient for the current project.

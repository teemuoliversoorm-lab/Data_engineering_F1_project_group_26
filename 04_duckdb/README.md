Running:
# docker build -t f1-duckdb .\04_duckdb
# docker run --rm --env-file .env -v "${PWD}\04_duckdb\data:/data" f1-duckdb python compare_engines.py

This runs b) and d) business question queries (Q2 and Q4) and outputs a comparison table between PostreSQL and DuckDB. 

DuckDB was added as an optional analytical layer using a copy of the PostgreSQL Gold dimensional model. Refresh scripts replicate the Gold schema into native DuckDB tables, keeping DuckDB independent of the main pipeline. 

DuckDB is used as an optional analytical copy of the PostgreSQL Gold layer. Its columnar storage is well suited to the read-heavy query patterns in this project, where queries scan fact-table columns, join them with relatively small dimensions, and perform grouping and aggregation. This fits queries such as b, which aggregates position changes by team, and d, which joins lap-level data with event, driver, and result data to identify whether the pole-position driver retained the lead after lap 1. 

PostgreSQL remains the primary warehouse because the dataset is small, the pipeline also performs ingestion and dbt-managed transformations there, and PostgreSQL provides a convenient relational store for the full Bronze–Silver–Gold workflow. DuckDB is therefore used mainly as a lightweight columnar analytical layer rather than as a replacement for PostgreSQL. In our measurements, PostgreSQL was slightly faster for the tested queries because the dataset is too small for DuckDB's columnar advantages to clearly outweigh per-query overhead. The comparison therefore demonstrates the architectural fit of DuckDB for analytical workloads rather than a general performance advantage over PostgreSQL. 

To validate DuckDB, queries b and d were executed in both engines, producing identical results. Each query was run once for warm-up and five additional times per engine, with median execution times reported. PostgreSQL was faster for both queries, likely because the dataset is too small for DuckDB's columnar advantages to outweigh per-query overhead. These results should therefore not be treated as a general performance benchmark.  

| Query | Same result? | PostgreSQL median (ms) | DuckDB median (ms) |
|---|---|---|---|
| b – Team position change | Yes | 1.627 | 3.873 |
| d – Pole retained lead after lap 1 | Yes | 1.827 | 4.326 |

*Table 1. Query execution time comparison.*

Each query was run once for warm-up and five additional times per engine, with median execution times reported. PostgreSQL was faster for both queries, likely because the dataset is too small for DuckDB's columnar advantages to outweigh per-query overhead. These results should therefore not be treated as a general performance benchmark.

DuckDB introduces an additional Gold data copy that requires refreshing when PostgreSQL changes. However, it demonstrates that the same dimensional model can support a columnar analytical engine while producing identical results. DuckDB may become more beneficial as data volume and fact-table width increase, but PostgreSQL remains sufficient for the current project.

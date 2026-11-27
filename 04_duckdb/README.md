# DuckDB bonus (Project_2.md 2.5A — columnar engine)

Optional: never required for the main pipeline to build or run. See the root
[`README.md`](../README.md#duckdb-bonus) for how this fits into the DAGs.

## Running

```sh
docker compose --profile tools build duckdb   # build the image (also builds via the normal `--profile tools build`)
docker compose run --rm duckdb                # refresh the DuckDB copy of gold from PostgreSQL
docker compose run --rm duckdb-compare        # run Q2 and Q4 in both engines, write results below
```

Both commands bind-mount `./04_duckdb/data` into the container, so the
DuckDB file and the results land directly in this folder (committable to
git). The automatic version triggered by the `f1_ingestion` DAG
(`f1_duckdb_refresh`, fired without waiting for it to finish) instead uses an
internal Docker volume, since there's no host path to bind-mount from inside
that container — see `00_dags/f1_duckdb_refresh.py`.

This runs the Q2 and Q4 business questions and outputs a comparison table
between PostgreSQL and DuckDB.

DuckDB was added as an optional analytical layer using a copy of the PostgreSQL Gold dimensional model. Refresh scripts replicate the Gold schema into native DuckDB tables, keeping DuckDB independent of the main pipeline.

DuckDB is used as an optional analytical copy of the PostgreSQL Gold layer. Its columnar storage is well suited to the read-heavy query patterns in this project, where queries scan fact-table columns, join them with relatively small dimensions, and perform grouping and aggregation. This fits queries such as Q2, which aggregates position changes by team, and Q4, which joins lap-level data with event, driver, and result data to identify whether the pole-position driver retained the lead after lap 1.

PostgreSQL remains the primary warehouse because the dataset is small, the pipeline also performs ingestion and dbt-managed transformations there, and PostgreSQL provides a convenient relational store for the full Bronze–Silver–Gold workflow. DuckDB is therefore used mainly as a lightweight columnar analytical layer rather than as a replacement for PostgreSQL. In our measurements, PostgreSQL was slightly faster for the tested queries because the dataset is too small for DuckDB's columnar advantages to clearly outweigh per-query overhead. The comparison therefore demonstrates the architectural fit of DuckDB for analytical workloads rather than a general performance advantage over PostgreSQL.

To validate DuckDB, Q2 and Q4 were executed in both engines, producing identical results. Each query was run once for warm-up and five additional times per engine, with median execution times reported.

| Query | Same result? | PostgreSQL median (ms) | DuckDB median (ms) |
|---|---|---|---|
| Q2 – Team position change | Yes | 1.627 | 3.873 |
| Q4 – Pole retained lead after lap 1 | Yes | 1.827 | 4.326 |

*Table 1. Query execution time comparison. These are small-dataset results,
not a general performance benchmark — see discussion above.*

DuckDB introduces an additional Gold data copy that requires refreshing when PostgreSQL changes. However, it demonstrates that the same dimensional model can support a columnar analytical engine while producing identical results. DuckDB may become more beneficial as data volume and fact-table width increase, but PostgreSQL remains sufficient for the current project.

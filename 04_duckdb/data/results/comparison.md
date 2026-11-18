# PostgreSQL vs DuckDB

| Query | Same result? | Rows | PostgreSQL median (ms) | DuckDB median (ms) | PostgreSQL EXPLAIN execution (ms) |
|---|---:|---:|---:|---:|---:|
| Q2 - team position change | Yes | 10 | 1.627 | 3.873 | 0.159 |
| Q4 - pole retained lead after lap 1 | Yes | 1 | 1.827 | 4.326 | 0.088 |

# PostgreSQL vs DuckDB

| Query | Same result? | Result rows | PostgreSQL median (ms) | DuckDB median (ms) | PostgreSQL EXPLAIN execution (ms) |
|---|---:|---:|---:|---:|---:|
| Q2 - team position change | Yes | 12 | 2.221 | 3.945 | 0.676 |
| Q4 - pole retained lead after lap 1 | Yes | 32 | 4.222 | 6.198 | 0.521 |

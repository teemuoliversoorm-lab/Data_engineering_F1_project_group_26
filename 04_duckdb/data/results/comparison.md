# PostgreSQL vs DuckDB

| Query | Same result? | Result rows | PostgreSQL median (ms) | DuckDB median (ms) | PostgreSQL EXPLAIN execution (ms) |
|---|---:|---:|---:|---:|---:|
| Q2 - team position change | Yes | 21 | 1.320 | 5.143 | 0.942 |
| Q4 - P1 retained lead after lap 1 | Yes | 33 | 2.851 | 8.904 | 0.666 |

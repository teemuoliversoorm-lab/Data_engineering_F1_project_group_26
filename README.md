# F1 Analytics Data Platform

**Data Engineering Project 2 — Group 26**
University of Tartu, III Semester

An analytical pipeline that ingests Formula 1 qualifying/race data from the
[OpenF1 API](https://openf1.org/docs/), loads it through a bronze → silver →
gold medallion architecture in PostgreSQL (orchestrated by Airflow, transformed
by dbt), and answers six business questions about qualifying vs. race
performance for the 2025–2026 seasons.

**Team:** Ahto Kiil, Dagmar Läänemets, Georg Allikas, Kerttu Tilk, Teemu-Oliver Soorm

For the original Project 1 proposal (FastF1-based, before the source switch to
OpenF1), see [`Reports/Project_1_long_version.md`](Reports/Project_1_long_version.md).
For the Project 2 assignment brief, see [`Reports/Project_2.md`](Reports/Project_2.md).

---

## Business questions

1. How frequently does the pole position (P1) qualifier win the race?
2. Which teams gained or lost the most net positions across the season?
3. Which individual drivers gained or lost the most positions across the season?
4. How many times (and at which circuits) has the pole position driver maintained the lead at the end of lap 1?
5. What is the head-to-head qualifying position gap between teammates, and does it translate into final race results?
6. What is the largest qualifying time gap (and position) to pole from which a driver still managed to win the race?

---

## Repository layout

Top-level folders are numbered in pipeline order, so the data flow is visible
at a glance:

```
.
├── 00_dags/            Airflow DAG — the entry point you trigger
│   └── f1_ingestion.py
├── 01_ingestion/        Python: OpenF1 client, data-quality checks, bronze loader
│   ├── openf1.py         API client + field mapping
│   ├── quality.py        the data-quality check + parameter validation
│   ├── extract.py        pulls and adapts one session (laps + results)
│   ├── load_bronze.py    idempotent load into bronze
│   ├── cli.py             manual debugging entry point
│   └── sql/bronze_setup.sql
├── 02_dbt/              dbt project: bronze -> silver -> gold
│   └── models/{silver,gold}/
├── 03_queries/          the 6 business-question SQL queries (run against gold)
├── archive/             Project 1 legacy, not used by the running pipeline
├── docs/data_model.md   OpenF1 field mapping + known limitations
├── tests/test_pipeline.py
├── docker/              Dockerfiles for warehouse, airflow, dbt images
└── compose.yaml
```

---

## Architecture

```
  OpenF1 API
      |  laps, session_result (+ drivers, grid, position)
      v
  00_dags/  Airflow: extract -> check -> load, one session at a time
      |
      v
  bronze    PostgreSQL, raw JSONB, one row per driver/lap or driver/session
      |
      v
  02_dbt/   silver (typed views) -> gold (star schema: 2 facts, 4 dims)
      |
      v
  03_queries/  the 6 business-question SQL queries
```

1. **Source:** OpenF1's public historical API — `laps` and `session_result` are
   the two distinct datasets (different grain: per-lap vs. per-driver-per-session).
2. **Ingestion:** an Airflow DAG (`00_dags/f1_ingestion.py`) extracts, data-quality
   checks, and loads one `(season, round, session)` batch at a time. Re-running
   the same batch is a no-op in terms of row count — see [Idempotency](#idempotency).
3. **Storage:** PostgreSQL, chosen because the workload is relational joins across
   a handful of dimensions (driver/team/event) at season scale — no need for a
   columnar or distributed engine, and it's what dbt and Airflow both integrate
   with cleanly. Bronze/silver/gold are separate schemas in the same database.
4. **Transformation:** dbt builds silver (typed, cleaned views) and gold (the
   Project 1 star schema: 2 fact tables, 4 dimensions) from bronze.
5. **Serving:** the 6 business questions are plain SQL against gold (see
   [`03_queries/`](03_queries)). Dashboards are out of scope for this submission.

---

## Data sources

| Dataset | OpenF1 endpoint | Grain |
|---|---|---|
| Lap timing | `laps`, plus `position` for approximate lap-end position | One row per driver, per lap, per session |
| Session results | `session_result`, `starting_grid`, `drivers` | One row per driver, per session |

See [`docs/data_model.md`](docs/data_model.md) for the full field mapping and
known OpenF1 limitations (approximate lap positions, name-derived driver/team IDs).

---

## Data model

Gold is a star schema with two fact tables and four dimensions:

- **FactLap** — one row per driver, lap, session, event
- **FactDriverEventResult** — one row per driver, per race event (qualifying joined in)
- **DimDriver**, **DimTeam**, **DimEvent** (Type 1), **DimDriverTeamAssignment** (Type 2, tracks mid-season team changes)

Full column-level data dictionary and SCD justification:
[`Reports/Project_1_long_version.md`](Reports/Project_1_long_version.md#5-data-model)
(unchanged from Project 1 — see that doc for why).

---

## Setup

Requires Docker Desktop (Linux containers) or Docker Engine with Compose v2.

```sh
cp .env.example .env        # edit passwords before first startup
docker compose --profile tools build
docker compose up -d
docker compose ps           # wait for warehouse / airflow-webserver / airflow-scheduler healthy
```

Airflow UI: http://localhost:8080 (credentials from `.env`).
PostgreSQL: `localhost:5433` (credentials from `.env`); bronze/silver/gold are
separate schemas in the same `f1_warehouse` database. The DAG starts **paused**.

---

## Run the pipeline

**One session, via the CLI** (useful for debugging without the Airflow UI):

```sh
docker compose run --rm pipeline -m ingestion.cli ingest \
  --season 2025 --round 1 --session R --cutoff 2026-01-01
```

**Via Airflow:** unpause `f1_ingestion` in the UI, or trigger it with a run
configuration:

```json
{"season": 2025, "rounds": [1, 2, 3], "sessions": ["Q", "R"], "cutoff": "2026-01-01"}
```

Each `(round, session)` pair is extracted, data-quality checked, and loaded
independently — one failing session doesn't block the others. Re-running the
same configuration is safe (see [Idempotency](#idempotency)).

**Build the warehouse (dbt):**

```sh
docker compose run --rm dbt build
```

---

## Idempotency

`01_ingestion/load_bronze.py` loads one session batch by deleting any existing
rows for that `(season, round, session_type)` and inserting the freshly
extracted rows, in a single transaction. Running the same input again produces
the same final row counts — no duplicates, no failure. This was verified with
a real session loaded twice (928 laps / 20 results both times) and is covered
by `tests/test_pipeline.py::IdempotencyTests`.

---

## Verify

```sh
# Fast tests (data-quality checks + field mapping, no database needed)
docker compose run --rm pipeline -m unittest tests.test_pipeline -v

# Full suite, including the idempotency test against a real temporary database
docker compose run --rm -e F1_TEST_POSTGRES=1 pipeline -m unittest tests.test_pipeline -v

# dbt tests (27 tests: uniqueness, not-null, relationships, fact grains, lap-time validity)
docker compose run --rm dbt build
```

---

## Query results

Results below are from a real run on **6 November 2026**, covering the full
2025 season (24 rounds) and every completed 2026 round through Mexico City
(round 19 of 23; round 20, Brazil, is 8 November 2026 — not yet run). Two
known OpenF1 gaps affect coverage: **Baku 2025 Qualifying** has no published
lap data, so that one Q session (and its qualifying times) never loaded.

**Q1 — how often does pole win the race?**

| races | pole wins | win % |
|---|---|---|
| 42 | 28 | 66.67 |

**Q2 — net position change by team** (2025+2026 combined — see note below):

| team | net positions | | team | net positions |
|---|---|---|---|---|
| Haas F1 Team | +186 | | Audi | +39 |
| Williams | +120 | | Alpine | +30 |
| Aston Martin | +111 | | Racing Bulls | +15 |
| Red Bull Racing | +105 | | Mercedes | +1 |
| Kick Sauber | +83 | | McLaren | -10 |
| Cadillac | +59 | | Ferrari | +56 |

**Q3 — net position change by driver** (top 5 / bottom 2 of 25):

| driver | net positions |
|---|---|
| Nico Hülkenberg | +93 |
| Oliver Bearman | +90 |
| Esteban Ocon | +88 |
| Alexander Albon | +84 |
| Lance Stroll | +74 |
| ... | ... |
| Pierre Gasly | -12 |
| George Russell | -20 |

**Q4 — pole sitter leading after lap 1** (31 of 42 races; full list in
[`03_queries/query_04_lap1_leader.sql`](03_queries/query_04_lap1_leader.sql) output):
Lando Norris and Max Verstappen lead this list with 6 and 5 races respectively.

**Q5 — teammate qualifying/race gap:** 897 driver-event rows (one per
driver/team/event, both seasons); see
[`03_queries/query_05_teammate_comparison.sql`](03_queries/query_05_teammate_comparison.sql)
for the full table. `query_05` doesn't label rows by season either — the same
cross-season limitation as Q2/Q3/Q4 below.

**Q6 — largest qualifying gap to pole among race winners:**

| event | season | driver | qualifying position | gap to pole (s) |
|---|---|---|---|---|
| Las Vegas Grand Prix | 2025 | Max Verstappen | 2 | 0.323 |

This answer was wrong before a fix found during this round of testing: the one
race with an unknown qualifying gap (Baku 2025, see above) was being picked as
the "largest" gap, because PostgreSQL sorts `NULL` as larger than any value in
`ORDER BY ... DESC`. Fixed in `03_queries/query_06_biggest_gap_win.sql` by
excluding rows with no known gap.

**Known limitations in these results** (business-rule refinement, not yet
done — see `Reports/Report.pdf` for the full writeup):
- Q2, Q3 and Q5 combine 2025 and 2026 into one set of numbers rather than
  reporting per season.
- Q4 reports only races where the pole sitter *did* lead lap 1, not a
  per-season rate with an explicit denominator.

Full output for all 6 queries, plus Airflow DAG graph and dbt dependency
screenshots, are in `Reports/Report.pdf`.

---

## Troubleshooting

**`dependency failed to start: container warehouse is unhealthy`** — usually a
leftover half-initialized volume from an interrupted first start. Fix with
`docker compose down` then `docker volume rm data_engineering_f1_project_group_26_warehouse-data`
and start again (this discards local warehouse data, which is rebuilt from
scratch by re-running the DAG and `dbt build`).

**`OpenF1 request failed after 3 attempts`** — OpenF1 occasionally rate-limits
or has brief outages; re-run the same command, extraction is safe to retry.

**Port already in use** — change `POSTGRES_PORT` / `AIRFLOW_PORT` in `.env`.

---

## LLM disclosure

- 
- 
- 

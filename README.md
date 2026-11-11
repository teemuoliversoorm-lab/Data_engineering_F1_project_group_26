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

**Fully automatic (default):** unpause `f1_ingestion` in the Airflow UI and
trigger it with no configuration (or wait for the weekly schedule). It
discovers every completed round for `seasons: [2025, 2026]` from OpenF1's own
calendar and skips anything already loaded in bronze, so each run — whether
weekly or manually triggered — only fetches genuinely new rounds:

```sh
docker compose run --rm pipeline -m unittest -v  # optional: verify first
# then trigger f1_ingestion from the Airflow UI with an empty / default run configuration
```

Runs automatically **every Monday 07:00 Europe/Tallinn** (`schedule` in
`00_dags/f1_ingestion.py`). You can override the defaults with a run
configuration if needed, e.g. to backfill only one season:

```json
{"seasons": [2025], "sessions": ["Q", "R"], "cutoff": "2026-11-06"}
```

**One specific session, via the CLI** (useful for debugging, or to force a
refresh of a session already in bronze — the DAG's weekly run won't
re-fetch something it's already loaded, but the CLI always will):

```sh
docker compose run --rm pipeline -m ingestion.cli ingest \
  --season 2025 --round 1 --session R --cutoff 2026-01-01
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

Two separate layers, in order:

1. **The DAG skips what's already loaded.** `plan_sessions` in
   `00_dags/f1_ingestion.py` checks bronze first and only builds extract/check/load
   tasks for `(season, round, session_type)` combinations that aren't there yet
   (see [Run the pipeline](#run-the-pipeline)). So triggering the DAG normally
   is a no-op for rounds you already have — nothing is re-extracted or re-loaded.
2. **When a batch *is* loaded, it's a safe replace.** `01_ingestion/load_bronze.py`
   loads one session batch by deleting any existing rows for that
   `(season, round, session_type)` and inserting the freshly extracted rows, in
   a single transaction. Running the same input again produces the same final
   row counts — no duplicates, no failure. This was verified with a real
   session loaded twice (928 laps / 20 results both times) and is covered by
   `tests/test_pipeline.py::IdempotencyTests`.

To force a genuine refresh of a round already in bronze (e.g. after an OpenF1
correction), use the CLI directly — it skips layer 1 entirely and always
re-extracts and re-loads: `ingestion.cli ingest --season ... --round ...
--session ... --cutoff ...`.

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
Run the business questions queries ([`03_queries/`]) against Gold while using Visual Studio Code Database (or other) extension or DBeaver or your preferred tool.

Results below are from a real run on **11 November 2026**, covering the full
2025 season (24 rounds) and every completed 2026 round through Brazil
(round 20 of 23). Two
known OpenF1 gaps affect coverage: **Baku 2025 Qualifying** has no published
lap data, so that one Q session (and its qualifying times) never loaded.

**Q1 — how often does pole win the race?**

| races | pole wins | win % |
|---|---|---|
| 43 | 29 | 67.44 |

**Q2 — net position change by team** (2025+2026 combined):

| team | net positions | | team | net positions |
|---|---|---|---|---|
| Haas F1 Team | +166 | | Cadillac | +63 |
| Red Bull Racing | +126 | | Audi | +36 |
| Aston Martin | +107 | | Alpine | +31 |
| Williams | +107 | | Racing Bulls | +18 |
| Kick Sauber | +83 | | Mercedes | +1 |
| Ferrari | +73 | | McLaren | -6 |

**Q3 — net position change by driver** (all drivers, 2025+2026 combined):

| driver | net positions | | driver | net positions |
|---|---|---|---|---|
| Esteban Ocon | +88 | | Carlos Sainz | +29 |
| Nico Hülkenberg | +87 | | Yuki Tsunoda | +21 |
| Oliver Bearman | +79 | | Kimi Antonelli | +18 |
| Alexander Albon | +78 | | Valtteri Bottas | +14 |
| Lance Stroll | +74 | | Ryo Hirakawa | +8 |
| Lewis Hamilton | +66 | | Charles Leclerc | +7 |
| Sergio Perez | +49 | | Jack Doohan | +6 |
| Liam Lawson | +46 | | Lando Norris | +1 |
| Max Verstappen | +46 | | Isack Hadjar | -4 |
| Franco Colapinto | +36 | | Oscar Piastri | -7 |
| Arvid Lindblad | +35 | | Rafael Camara | -9 |
| Fernando Alonso | +33 | | Pierre Gasly | -11 |
| Gabriel Bortoleto | +32 | | George Russell | -17 |

**Q4 — pole sitter leading after lap 1** (31 of 43 races; 2025 left, 2026 right):

| season | event | driver | | season | event | driver |
|---|---|---|---|---|---|---|
| 2025 | Abu Dhabi Grand Prix | Max Verstappen | | 2026 | Austrian Grand Prix | George Russell |
| 2025 | Australian Grand Prix | Lando Norris | | 2026 | Azerbaijan Grand Prix | George Russell |
| 2025 | Austrian Grand Prix | Lando Norris | | 2026 | Bahrain Grand Prix | Max Verstappen |
| 2025 | Bahrain Grand Prix | Oscar Piastri | | 2026 | Barcelona Grand Prix | George Russell |
| 2025 | Belgian Grand Prix | Lando Norris | | 2026 | Belgian Grand Prix | Kimi Antonelli |
| 2025 | British Grand Prix | Max Verstappen | | 2026 | Dutch Grand Prix | Lando Norris |
| 2025 | Canadian Grand Prix | George Russell | | 2026 | Italian Grand Prix | Pierre Gasly |
| 2025 | Chinese Grand Prix | Oscar Piastri | | 2026 | Monaco Grand Prix | Kimi Antonelli |
| 2025 | Dutch Grand Prix | Oscar Piastri | | 2026 | Singapore Grand Prix | Max Verstappen |
| 2025 | Hungarian Grand Prix | Charles Leclerc | | 2026 | Spanish Grand Prix | Lando Norris |
| 2025 | Italian Grand Prix | Max Verstappen | | 2026 | United States Grand Prix | Kimi Antonelli |
| 2025 | Japanese Grand Prix | Max Verstappen | | | | |
| 2025 | Mexico City Grand Prix | Lando Norris | | | | |
| 2025 | Monaco Grand Prix | Lando Norris | | | | |
| 2025 | Qatar Grand Prix | Oscar Piastri | | | | |
| 2025 | São Paulo Grand Prix | Lando Norris | | | | |
| 2025 | Saudi Arabian Grand Prix | Max Verstappen | | | | |
| 2025 | Singapore Grand Prix | George Russell | | | | |
| 2025 | Spanish Grand Prix | Oscar Piastri | | | | |
| 2025 | United States Grand Prix | Max Verstappen | | | | |

**Q5 — teammate qualifying/race gap:** 919 driver-event rows (one per
driver/team/event, both seasons); see
[`03_queries/query_05_teammate_comparison.sql`](03_queries/query_05_teammate_comparison.sql)
for the full table. First rows of the output (2025 Abu Dhabi Grand Prix):

| season | event | team | driver | qualifying pos | finish pos | gap to pole (s) |
|---|---|---|---|---|---|---|
| 2025 | Abu Dhabi Grand Prix | Alpine | Pierre Gasly | 19 | 19 | 1.261 |
| 2025 | Abu Dhabi Grand Prix | Alpine | Franco Colapinto | 20 | 20 | 1.683 |
| 2025 | Abu Dhabi Grand Prix | Aston Martin | Lance Stroll | 15 | 10 | 0.890 |
| 2025 | Abu Dhabi Grand Prix | Aston Martin | Fernando Alonso | 6 | 6 | 0.695 |
| 2025 | Abu Dhabi Grand Prix | Ferrari | Lewis Hamilton | 16 | 8 | 1.187 |
| 2025 | Abu Dhabi Grand Prix | Ferrari | Charles Leclerc | 5 | 4 | 0.523 |
| 2025 | Abu Dhabi Grand Prix | Haas F1 Team | Esteban Ocon | 8 | 7 | 0.706 |
| 2025 | Abu Dhabi Grand Prix | Haas F1 Team | Oliver Bearman | 11 | 12 | 0.834 |
| 2025 | Abu Dhabi Grand Prix | Kick Sauber | Nico Hülkenberg | 18 | 9 | 1.243 |
| 2025 | Abu Dhabi Grand Prix | Kick Sauber | Gabriel Bortoleto | 7 | 11 | 0.697 |

**Q6 — largest qualifying gap to pole among race winners** (first 5):

| event | season | driver | qualifying position | gap to pole (s) |
|---|---|---|---|---|
| Las Vegas Grand Prix | 2025 | Max Verstappen | 2 | 0.323 |
| Italian Grand Prix | 2026 | Kimi Antonelli | 7 | 0.307 |
| Qatar Grand Prix | 2025 | Max Verstappen | 3 | 0.264 |
| Mexico City Grand Prix | 2026 | Max Verstappen | 4 | 0.178 |
| British Grand Prix | 2026 | Charles Leclerc | 2 | 0.175 |

This answer was wrong before a fix found during this round of testing: the one
race with an unknown qualifying gap (Baku 2025, see above) was being picked as
the "largest" gap, because PostgreSQL sorts `NULL` as larger than any value in
`ORDER BY ... DESC`. Fixed in `03_queries/query_06_biggest_gap_win.sql` by
excluding rows with no known gap.

---

## Troubleshooting

**`dependency failed to start: container warehouse is unhealthy`** — usually a
leftover half-initialized volume from an interrupted first start. Fix with
`docker compose down` then `docker volume rm data_engineering_f1_project_group_26_warehouse-data`
and start again (this discards local warehouse data, which is rebuilt from
scratch by re-running the DAG and `dbt build`).

**`OpenF1 request failed after 3 attempts`** — OpenF1 occasionally rate-limits
or has brief outages; re-run the same command, extraction is safe to retry.
If the underlying error is `401 Unauthorized: "Live F1 session in progress..."`,
OpenF1 blocks its **entire** public API (including past seasons) while any F1
session is live, not just real-time data. Wait for the session to end and
retry — see [`docs/data_model.md`](docs/data_model.md).

**Port already in use** — change `POSTGRES_PORT` / `AIRFLOW_PORT` in `.env`.

**DAG doesn't appear in the Airflow UI, even though services are healthy** —
check `docker compose logs airflow-scheduler` for `PermissionError` on
`/opt/airflow/logs/scheduler/...`. This happens if the volume was initialized
by an older version of `airflow-init`'s startup command (before it re-ran
`chown` after `airflow db migrate`). Fix by re-running `docker compose run
--rm airflow-init` followed by `docker compose restart airflow-scheduler
airflow-webserver` — or simplest, `docker compose down -v` and start fresh.

**Can't connect to PostgreSQL from a GUI client** — connect to `localhost:5433`
(not `5432`, which is the separate `airflow-db` instance with different
credentials), database `f1_warehouse`, user/password from `.env`'s
`POSTGRES_USER`/`POSTGRES_PASSWORD`.

---

## LLM disclosure

- 
- 
- 

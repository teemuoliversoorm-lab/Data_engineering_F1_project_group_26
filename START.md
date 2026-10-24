# Start and check the F1 project

Commands below use **Ubuntu WSL / Bash**, from this repository. Start Docker
Desktop with Linux containers and enable its Ubuntu WSL integration. Docker
Engine with Compose v2 also works. No local Python, Airflow or PostgreSQL install
is needed. Allow roughly 4 GB of Docker memory and internet access for first builds.
Wait until the engine is ready (`docker info` succeeds) before starting containers.

## Start containers

```bash
cd ~/Data_engineering_F1_project_group_26
if [ ! -f .env ]; then cp .env.example .env; fi
```

On a first installation, edit `.env` to choose passwords and an Airflow secret key;
keep `AIRFLOW_DB_PASSWORD` URL-safe. Keep an existing `.env`. Then start:

```bash
docker compose --env-file .env up -d --build
docker compose --env-file .env ps --all
```

The four services `warehouse`, `airflow-db`, `airflow-webserver` and
`airflow-scheduler` should become healthy. `airflow-init` must exit with code 0.
If startup fails, inspect:

```bash
docker compose --env-file .env logs --tail 80 airflow-init warehouse airflow-webserver airflow-scheduler
```

Startup initializes/migrates schemas; it does not ingest data or run dbt. Existing
named volumes retain data. A fresh installation starts empty. Code is copied
into images, so rebuild after source changes.

## Credentials

Use your `.env` values; these are the `.env.example` defaults:

| Service | Address | Username | Password |
| --- | --- | --- | --- |
| Airflow | http://localhost:8080 | admin | admin_local_change_me |
| PostgreSQL warehouse | localhost:5433, database `f1_warehouse` | f1 | f1_local_change_me |

Ports, database and usernames are configurable in `.env`. Existing PostgreSQL
volumes and Airflow admin accounts retain their original passwords; editing
`.env` does not rotate them. `airflow-db` contains orchestration metadata, not F1 data.

## Load or resume historical data

The existing local volumes contain the available 2025 Q/R data and 2026 rounds
1-17, through Singapore on 11 October (exclusive cutoff **22 October 2026**).
These commands also work on an empty installation:

```bash
docker compose --env-file .env run --rm pipeline scripts/smoke_dag.py \
  --season 2025 --rounds {1..24} --sessions Q R --cutoff 2026-01-01 \
  --reuse-landed --allow-missing-laps

docker compose --env-file .env run --rm pipeline scripts/smoke_dag.py \
  --season 2026 --rounds {1..17} --sessions Q R --cutoff 2026-10-22 \
  --reuse-landed --allow-missing-laps
```

`--reuse-landed` validates saved bundles and fetches historical API data only for
sessions without a saved bundle. Identical loads return `unchanged`. Omit this
flag to check for upstream corrections. `--allow-missing-laps` explicitly records
unavailable laps while loading available results; it never invents laps or erases
previously loaded laps. Partial classifications retain observed participants with
null result metrics. See [coverage and issues](README_Phase2.md).

For other rounds/cutoffs, inspect the source calendar first:

```bash
docker compose --env-file .env run --rm pipeline -m ingestion.cli schedule --season 2026
```

Cutoffs are exclusive UTC dates; only completed historical sessions are accepted.
The Airflow DAG `f1_ingestion` starts paused. Its Tuesday 06:00 UTC schedule uses
**2025 round 1** defaults; it does not select new race weekends automatically.
Keep it paused unless you have configured the intended recurring coverage.

After loading or correcting data, build the silver views and gold tables:

```bash
docker compose --env-file .env run --rm --build dbt build
```

## Check the database

Open psql (substitute your user/database if changed):

```bash
docker compose --env-file .env exec warehouse psql -U f1 -d f1_warehouse
```

Run inside psql:

```sql
\dt bronze.*
\dv silver.*
\dt gold.*

SELECT season, session_type, dataset, count(*) AS audited_sessions,
       count(*) FILTER (WHERE row_count > 0) AS sessions_with_data,
       sum(row_count) AS loaded_rows
FROM bronze.load_audit
GROUP BY season, session_type, dataset ORDER BY 1, 2, 3;

-- Actual source classifications, excluding unclassified participant records.
SELECT season, session_type, sum(jsonb_array_length(source_records)) AS source_rows
FROM bronze.source_responses WHERE source_query LIKE 'session_result?%'
GROUP BY season, session_type ORDER BY 1, 2;

SELECT season, round_number, session_type, dataset,
       metadata->'dataset_status'->>dataset AS availability,
       metadata->'participants_without_classification' AS unclassified_participants
FROM bronze.load_audit
WHERE metadata->'dataset_status'->>dataset IN ('unavailable', 'partial')
ORDER BY 1, 2, 3, 4;

SELECT count(*) AS events FROM gold.dimevent;
SELECT count(*) AS laps FROM gold.factlap;
SELECT count(*) AS race_driver_rows FROM gold.factdrivereventresult;
\q
```

Gold queries require a successful `dbt build`. Compare results with the dated
counts in [README_Phase2](README_Phase2.md). For the draft analytical SQL, set
`SET search_path TO gold, public;` in psql; business-rule validation remains in 2.3.

## Stop, keeping data

```bash
docker compose --env-file .env down
```

Database, landing, request-state and log volumes are preserved. Do not add
`--volumes` when you want to keep them.

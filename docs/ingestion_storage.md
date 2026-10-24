# Ingestion and PostgreSQL storage

The project uses OpenF1's public historical API and PostgreSQL. Source access
requires no F1 TV subscription or OpenF1 account for completed historical sessions.
See [OpenF1 documentation](https://openf1.org/docs/) and the
[data contract](data_contract.md) for mappings and limitations.

## Database choice

PostgreSQL fits this project's structured lap/result data and repeated analytical
joins across driver, team and event dimensions. JSONB preserves source fields;
typed composite keys, constraints and indexes support selective session refreshes.
Transactions and advisory locks make corrections, retries and concurrent loads
safe. The expected season-scale volumes fit a single local database, avoiding
extra distributed infrastructure. dbt-postgres supports the dimensional models.
Docker named volumes preserve data independently of containers.

## Components and layers

- One warehouse PostgreSQL service: bronze/silver/gold schemas.
- Separate PostgreSQL database service for Airflow metadata.
- One shared image for Airflow init, webserver, scheduler and pipeline CLI.
- Separate dbt image for manual transformations.
- Shared landing, request-throttle state and log volumes.

Airflow tasks: `plan_sessions -> mapped ingest_session(extract -> check -> load)`.
Mapping the group preserves dependencies within each session; another extraction's
failure cannot block successful session loads.
Extraction uses OpenF1 laps and session_result, enriched with driver/grid/position
and calendar metadata. Missing laps fail by default. Explicit `allow_missing_laps`
loads available classifications while flagging missing lap data in metadata/audit.
A partial source can never replace previously loaded laps. Participants actually
observed in laps but omitted from a classification retain null result metrics
and an explicit partial-classification flag. Source classification counts remain
separate from adapted participant counts. Other required inputs
must still be nonempty.

| Layer | Physical storage and ownership |
| --- | --- |
| Bronze | Airflow-owned source_responses JSONB, laps, session_results and load_audit tables |
| Silver | dbt-owned typed laps and session_results views |
| Gold | dbt-owned four dimension and two fact tables with grain/join indexes |

Original API responses are preserved separately from adapted compatibility fields.
All source fields, source URLs and hashes remain available for later dbt work.
The additive bronze schema is applied by airflow-init on startup, including
existing warehouse volumes. The old Project 1 sample SQL is not loaded here.

## Updates and recovery

A load transaction takes an advisory lock for season/round/session. Identical
hashes and stable metadata are a no-op; timestamp/cutoff-only changes do not
reload. Corrections atomically replace raw responses, both datasets and audit
rows for that session. Other sessions remain intact. A database failure rolls
the entire batch back, including raw provenance.

Retry failed extraction after resolving source availability. Retry a failed
load from its existing manifest without requesting the API again.
The API client spaces requests by 2.2 seconds across shared-volume processes and
handles 429/temporary failures. Airflow production tasks retry twice; the local
smoke command defaults to zero task retries to avoid long diagnostic loops.

Run `dbt build` separately after ingestion. Bronze updates incrementally by
session; gold currently rebuilds from all bronze data. Section 2.4's automatic
dbt task is not implemented.

## Verification

The OpenF1 pipeline has 46 passing Python tests, including actual Airflow DAG
parsing and isolated PostgreSQL tests. Cases include duplicate/range validation,
raw-file checksums, Q1/Q2/Q3 mapping, stable driver identity across number changes,
grid fallback provenance, bounded HTTP retries, concurrent loads, corrections,
no-op audit timestamps, partial-source protection, validated-bundle resume,
independent mapped-session loads, source retirement codes, explicit participants
without classifications and rollback of raw plus adapted records.

Real source coverage and counts are recorded in [README_Phase2](../README_Phase2.md).
The earlier synthetic tests proved mechanics only; normal ingestion never
substitutes fixtures. Lap-position approximation and name-based identity limits
are documented in the [contract](data_contract.md).

Use [START](../START.md) for startup, credentials and inspection. Stop with
`docker compose --env-file .env down`; volumes are preserved.

## Primary references

- [OpenF1 API](https://openf1.org/docs/)
- [PostgreSQL advisory locks](https://www.postgresql.org/docs/17/explicit-locking.html#ADVISORY-LOCKS)
- [Airflow Docker setup](https://airflow.apache.org/docs/apache-airflow/2.11.1/howto/docker-compose/)
- [dbt schema configuration](https://docs.getdbt.com/reference/resource-configs/schema)

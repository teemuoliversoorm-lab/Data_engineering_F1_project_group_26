# OpenF1 ingestion contract

The active source is [OpenF1](https://openf1.org/docs/). Historical data since
2023 is public and requires no account. The previous FastF1 extractor has been
replaced. The original Project 1 proposal remains a record of the earlier plan.

## Coverage and selection

- Parameters: season, championship round(s), Q/R sessions, exclusive UTC cutoff.
- Round numbers are derived by sorting non-cancelled main `Race` sessions in the
  OpenF1 season calendar. Testing and sprint sessions are excluded. Inspect
  `python -m ingestion.cli schedule --season 2025` before a backfill. Calendar
  omissions could affect ordinal round mapping; provider meeting/session keys
  are also recorded so mappings can be audited.
- A session must end before both the cutoff date and today in UTC. Requests for
  missing, cancelled, ongoing or ambiguous sessions fail without publishing.
- By default, missing required laps also fail the session. Explicitly enabling
  `allow_missing_laps` permits an empty lap response while preserving available
  results. Metadata records `dataset_status.laps = unavailable`, and the audit
  stores row_count = 0. Other empty required datasets still fail. This cannot
  overwrite previously loaded lap rows. No lap records are fabricated.
- Known 2025 source gap: Azerbaijan qualifying, round 17/session 9900. Results
  are available; laps return no records. Coverage queries must distinguish
  audited sessions from sessions with data.
- Explicit `reuse_landed` resumes from the newest validated local bundle after
  checking provenance, hashes and session end/cutoff. It does not refresh data.
  Omit it when checking for upstream corrections or newly available laps.
- One season/round/session is an atomic load unit containing laps and results.
  Other successfully loaded sessions survive a later task failure.

## Sources and preservation

The two primary datasets are `laps` and `session_result`. Supporting requests
supply `sessions`, `meetings`, `drivers`, `position` and `starting_grid`.
The client spaces requests by at least 2.2 seconds using a shared volume lock,
and retries HTTP 429 and transient server/network failures with bounded waits.
The scheduled Airflow DAG also has two task retries. The local smoke script
defaults to zero task retries for an immediate diagnostic result.

Each attempt writes:

```text
<data>/<season>/<round>/<Q|R>/<UUID>/
  openf1_raw.json
  laps.json
  session_results.json
  manifest.json
```

`openf1_raw.json` preserves every returned field and value in every response
array, with its request URL. Record ordering is canonicalized for stable hashes.
These same raw arrays load into `bronze.source_responses` as JSONB.
This preserves parsed JSON values, rather than exact HTTP response bytes.

The other two files contain explicitly adapted compatibility records for the
existing warehouse/dbt contract. Their capitalized names are internal field names,
not OpenF1's native names. Each record also retains its native OpenF1 lap/result
under `_openf1`. Raw driver, grid, position and metadata responses remain in
`bronze.source_responses`, so derived fields can be traced and reprocessed.

The manifest contains SHA-256 hashes, counts, field names, provider and adapter
version, extraction time, cutoff, source keys and event metadata. It is published
last. Raw hashes are checked before any database mutation. Landing files are
retained in a Docker volume for replay.

## Field mapping and limits

| Internal field | OpenF1 mapping |
| --- | --- |
| DriverNumber / LapNumber | driver_number / lap_number |
| LapTime | lap_duration seconds serialized as an ISO duration |
| DriverId | `openf1:` plus normalized full_name, independent of car number |
| TeamId | `openf1:` plus normalized team_name |
| FullName / Abbreviation / TeamName | drivers full_name / name_acronym / team_name |
| Position in results | Numeric session_result.position; text retirement/status codes map to null rank and retain their source code |
| Q1 / Q2 / Q3 | qualifying session_result.duration[0..2], in seconds |
| GridPosition | starting_grid.position; missing grid rows remain null |
| Points | session_result.points when present; otherwise null |
| Status | Explicit source status code, else dsq/dns/dnf flags; unknown remains null |
| ClassificationAvailable | false for participants observed in laps but absent from session_result; true for actual source classification rows |
| TrackStatus / Time | null: no equivalent is inferred |
| Position on race laps | last position observation at or before approximate lap end |
| CountryCode | drivers.country_code when supplied, otherwise null |

OpenF1 sometimes attaches a historical starting grid to the qualifying key.
Extraction first queries the race key, then the corresponding qualifying key if
the race query is empty. The actual key used is recorded as `grid_session_key`.
Qualifying classifications are never used as a substitute for a missing grid.

**Lap positions are approximate.** OpenF1's lap start timestamps are approximate,
and position changes are timestamped observations. Only completed laps with a
start time and duration receive an as-of position. Its method and observation
time are exposed in silver/gold. Qualifying lap positions, incomplete laps and
missing observations remain null. Validate lap-1 leadership conclusions against
a trusted reference before treating that business metric as exact.

OpenF1 can omit a lap participant from the session_result response. Their actual
driver metadata and lap observations are retained in a compatibility participant
record with `ClassificationAvailable = false`, `_openf1 = null`, the original
driver metadata in `_openf1_driver`, and null classification metrics. This is
not an inferred classification. Metadata marks session_results as `partial`,
lists `participants_without_classification` and records
`source_classification_rows` separately from the adapted participant count.
Silver/gold expose classification availability so analyses can exclude unknown
outcomes. Legacy bundles without this field contain only actual classifications.

Textual result positions such as `RT` are preserved under the original raw record
and `PositionStatusCode`, with null numeric Position and explicit Status. Unknown
codes still fail validation. A textual retirement is not converted to a guessed
finishing/qualifying rank.

OpenF1 does not supply immutable driver/constructor IDs. Name-derived IDs survive
car-number changes but need curated aliases if a name or constructor branding
changes. Track status is currently unavailable; source race-control data could
support a later enrichment. No points or grid positions are guessed.

## Validation and idempotency

Primary keys: laps = season/round/session/driver/lap; results =
season/round/session/driver. Stable driver IDs must also be unique per session.
Reject empty required datasets, missing identity/team fields, duplicate keys,
invalid lap numbers, positions outside 1..24 (grid also allows 0), unmatched
lap drivers and checksum/count mismatches. Missing metrics remain null.

Airflow maps the whole extract/check/load group per session so another session's
failed extraction cannot block its loads. PostgreSQL takes a per-session advisory lock. In one transaction it replaces raw
responses, adapted laps/results and audit rows. Identical content and stable
metadata return `unchanged`, preserving all loaded timestamps. Extraction time
and cutoff alone do not reload. Raw-only corrections do reload; failures roll
back all three storage tables and the audit. This is covered by integration tests.

## Layers

Bronze holds original response arrays, adapted records and provenance. dbt owns
silver typed views and the gold star: four dimensions and two facts. Existing
field names, ISO duration conversion and grains are retained. Gold rebuilds from
all bronze data; incremental dbt models and automatic Airflow-to-dbt execution
are later work.

Driver/team/event attributes use Type 1. Driver-team assignment history uses
Type 2 half-open intervals based on observed race-event dates, preferring R to Q.
FactLap is one event/session/driver/lap. FactDriverEventResult is one main-race
event/driver joined to qualifying. Qualifying time is Q3, else Q2, else Q1;
cross-stage gap comparisons remain explicitly identified.

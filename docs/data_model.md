# OpenF1 field mapping and known limitations

How raw OpenF1 fields become the warehouse's field names (see
`01_ingestion/openf1.py:adapt()` and `02_dbt/models/silver/`).

## Field mapping

| Warehouse field | OpenF1 source |
|---|---|
| `DriverId` | `openf1:` + normalized `drivers.full_name` (no immutable id exists) |
| `TeamId` | `openf1:` + normalized `drivers.team_name` |
| `LapTime`, `Q1`/`Q2`/`Q3` | seconds converted to an ISO-8601 duration, e.g. `92.5` → `PT92.5S` |
| `GridPosition` | `starting_grid.position`; null if no grid row exists |
| `Position` (session result) | numeric `session_result.position`, or null with `PositionStatusCode` set for a textual code (`DNF`, `DSQ`, ...) |
| `ClassificationAvailable` | `true` if the driver has an actual `session_result` row; `false` if they only appear in `laps` (e.g. retired before classification) |
| `Position` (lap) | **approximate** — see below |

## Known limitations

- **OpenF1 blocks ALL requests (even for past seasons) while any F1 session is
  live.** Every endpoint returns `401 Unauthorized` with
  `"Live F1 session in progress. Global API access (including past sessions)
  is restricted to authenticated users until the session ends."` This is not
  a bug in this pipeline — `ingestion/openf1.py` surfaces it as
  `SourceUnavailableError` and the affected task simply fails and can be
  retried once the session ends (practice/qualifying/race weekends are the
  main windows this happens). Airflow's task retries (2, with a 2-minute
  delay) may not be enough to outlast a full session; re-trigger manually
  afterward if a scheduled run fails for this reason.
- **Lap-end position is approximate.** OpenF1 has no per-lap position field.
  The pipeline takes the most recent `position` sample at or before each lap's
  end time (`PositionMethod: nearest_sample_at_or_after_lap_end`). This is
  good enough for business question 4 (lap-1 leader) but is not an exact
  official classification.
- **Driver/team IDs are name-derived**, not immutable OpenF1 ids. A name or
  team-branding change between seasons would need a manual alias — not
  currently handled.
- **`TrackStatus` is always null** — OpenF1 doesn't expose it on the `laps`
  endpoint used here.
- **Qualifying time comparisons can cross stages.** `QualifyingGapToPole` in
  gold compares `coalesce(Q3, Q2, Q1)` for both drivers; if one reached Q3 and
  the other was eliminated in Q1, the gap reflects different track conditions.
  `SameStageComparison` in `factdrivereventresult` flags this.
- **The weekly DAG never re-fetches a session already in bronze.**
  `plan_sessions` skips any `(season, round, session_type)` already present,
  so a scheduled run only costs API calls for genuinely new rounds. This
  means a later OpenF1 correction to an already-loaded session (e.g. a
  classification fix) won't be picked up automatically. Force a refresh of a
  specific session with the CLI: `ingestion.cli ingest --season ... --round
  ... --session ... --cutoff ...` (it always re-extracts and re-loads).

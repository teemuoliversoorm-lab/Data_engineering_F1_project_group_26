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

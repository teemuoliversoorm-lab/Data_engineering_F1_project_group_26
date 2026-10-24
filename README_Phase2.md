# Phase 2 status and next steps

Reviewed **24 October 2026** against `docs/project2-ut.pdf` and the six business
questions in [README](README.md). **2.1 and 2.2 are implemented and verified for
available historical data.** A working dbt foundation exists for 2.3; analytical
rules and documentation still need refinement. The data cutoff remains 22 October;
this review made no new API requests.

## What is done

| Requirement | Verified implementation |
| --- | --- |
| 2.1: distinct sources | OpenF1 `laps` and `session_result` are separately fetched datasets with different grains, from one API provider. Supporting calendar, driver, grid and position responses are also preserved with URLs and hashes. |
| 2.1: Airflow/parameters | Independently mapped extract/check/load groups; season, rounds, Q/R sessions and exclusive UTC cutoff parameters. |
| 2.1: quality | Identity/null, duplicate, range, relationship, checksum and row-count checks before loading. Explicit source-gap flags. |
| 2.1: idempotency | All **82 stored session bundles** replayed as `unchanged`; every bronze row fingerprint, including audit timestamps, stayed identical. Rollback, corrections and concurrency are tested. |
| 2.2: database choice | [PostgreSQL justification](docs/ingestion_storage.md): analytical joins, season-scale volume, JSONB provenance, transactional updates, local Docker deployment and dbt support. |
| 2.2: populated layers | Four bronze tables, two dbt silver views, four gold dimensions and two gold facts, with keys/constraints/indexes. Bronze refreshes by session; gold rebuilds fully. |

Dimensions cover driver, team, event and observed driver-team assignment history.
Fact grains are **event/session/driver/lap** and **main-race event/driver**, joined
to qualifying. Assignment dates reflect observations rather than contracts.
Models declare dependencies through `source()` and `ref()`.

## Loaded data

| Season | Weekends | Q laps | Race laps | Source Q classifications | Source race classifications |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2025 | 24 | 7,129 | 26,265 | 479 | 479 |
| 2026, before 22 October | 17 | 5,665 | 19,645 | 369 | 374 |

2026 ends at **Singapore, 11 October**, not season end. Rounds follow OpenF1's
chronological non-cancelled main-race calendar; source meeting/session keys remain.

Bronze totals: **58,704 laps, 1,701 source classifications plus two unclassified
participants, 533 raw snapshots and 164 audits**. Gold has **41 events, 58,704 fact
laps and 853 race driver-event rows**. These data reside in existing local Docker
volumes; a fresh installation starts empty.

## Issues

- **Missing sources:** Azerbaijan 2025 qualifying (round 17/session 9900) has results
  but no laps: Q lap coverage is 23/24 sessions. Australia 2026 qualifying (11230,
  driver 3) and Spain round 14 qualifying (11365, driver 18) omit lap participants
  from classification. Both are flagged `partial`; unknown metrics remain null.
  Source counts exclude those two adapted participant records.
- **Unknown numeric ranks:** 55 race rows in 2025 and 71 in 2026 have null finish
  and net-position delta. Most carry DNF/DNS/DSQ status. Two 2026 rows have false
  source status flags and are labelled `Finished` despite null rank. Do not infer
  ranks from status or silently treat these deltas as zero.
- **Metric limits:** lap-end positions are approximate; pole-sitter lap-one position
  is unknown in 2/41 events. Gap-to-pole compares different Q stages in 438 rows,
  explicitly flagged; all 41 current winners have a same-stage gap.
- **Other limits:** validate penalties/pit-lane handling in qualifying-key grid
  responses; curate name-derived identity aliases as needed. Track status is null.
  Weather, dashboards and OpenMetadata remain planned; the six current questions
  do not require weather.

## Business questions and 2.3 work

All six existing SQL files execute against gold with `search_path = gold, public`.
That does not establish business correctness:

| README question | Remaining query work |
| --- | --- |
| 1. Pole wins | Counts/rate exist; add season scope and an explicit eligible-race denominator. |
| 2. Team net positions | Currently combines seasons; group by season and report excluded/null outcomes. |
| 3. Driver net positions | Same season and null-outcome issues as question 2. |
| 4. Pole leading after lap one | Currently lists approximate matches; add circuit counts/rate, season and unknown coverage. Validate or label the approximation. |
| 5. Teammate comparison | Currently lists drivers; pair by event/team and calculate qualifying-position and race-result gaps. |
| 6. Largest gap winner | Add season scope, non-null/same-stage eligibility and tests of the chosen time definition. |

Implement 2.3 from the existing models:

1. Set eligibility/null rules, qualifying-time comparison and lap-one methodology;
   reconcile representative events against trusted classifications/grids.
2. Refine the six queries and required dbt fields using those rules, season/event
   identity, teammate pairing and coverage counts.
3. Complete silver/gold YAML field descriptions, grains, keys, units, lineage and
   null semantics. Extend tests for metric eligibility and teammate pairing.
4. Run `dbt build` and inspect all six query results on real data. Save reproducible
   analytical outputs and a dbt dependency diagram for submission.

**Verification:** 46 Python/PostgreSQL tests passed; dbt built **8 models**
(2 views, 6 tables) and passed **27 tests**. Startup reached healthy services with
`airflow-init` exit 0. No ingestion-code changes were needed during this review.

**After 2.3:** implement Airflow-to-dbt execution and end-to-end refresh in 2.4.
The paused Tuesday schedule still defaults to 2025 round 1; new-weekend selection
needs explicit configuration or calendar planning. Final screenshots, analytical
results and the assignment report remain deliverables.

See [START](START.md) for startup, credentials and database checks, and the
[data contract](docs/data_contract.md) for source mappings.

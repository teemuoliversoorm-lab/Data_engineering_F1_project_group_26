# F1 Analytics - Project 2 Implementation Plan


## Build the working pipeline

Project 2  |  Five equal portions  |  Submission: 19 November 2026, 23:59

Goal: build a repeatable F1 pipeline for 2025 and the completed part of 2026. Use the six existing business questions. Produce results that the team can explain and reproduce.

### Divide the work equally

| Member | Primary responsibility | Planned share |
| --- | --- | --- |
| Person 1 | Runtime, bronze loading, and Airflow | 30 h / 20% |
| Person 2 | FastF1 datasets, extraction, and checks | 30 h / 20% |
| Person 3 | dbt setup, silver, and dimensions | 30 h / 20% |
| Person 4 | Facts, metric rules, and model tests | 30 h / 20% |
| Person 5 | Queries, acceptance evidence, and report assembly | 30 h / 20% |

These assignments are proposed. Each portion includes tests, writing, and review. The 150-hour total is an estimate. Check remaining effort on 30 October and 6 November. Transfer bounded tasks if the portions become unequal.

### Keep the required scope clear

- Use Docker, Python, Airflow, PostgreSQL, and dbt. Separate bronze, silver, and gold.
- Keep both facts and all four dimensions. They support the existing questions and assignment history.
- Use SQL results as the serving layer. Defer dashboards, OpenMetadata, weather analysis, and bonus work until the required pipeline passes.

The five-page limit applies to the final course report. This eight-page implementation plan is a separate working document.


## Agree the data and architecture

Complete the shared contract by 23 October

### Use the two FastF1 datasets

Use session.laps and session.results from FastF1. These two distinct datasets satisfy the project source requirements. Keep the source design from Project 1. No additional data provider or source-approval step is required.

Dataset 1: session.laps. Extract lap timing, lap positions, and track status for qualifying and race sessions. Add season, round, and session metadata. Retain one record per driver, lap, session, and event.

Dataset 2: session.results. Extract qualifying and race classifications, driver and team identifiers, grid positions, qualifying times, and points. Keep one record per driver and session in bronze. Combine Q and R results only when building the event-summary fact.

Person 2 will profile both datasets by 22 October. Verify at least 1,000 distinct rows and eight useful columns in each dataset across the selected coverage. Count Q and R result rows separately. Record missing sessions. Do not count repeated loads toward the minimum.

### Use this processing path

| Stage | Content and responsibility |
| --- | --- |
| Extract | Person 2: two FastF1 extractors with a shared session loader and cache. Save separate raw files and manifests for laps and results. |
| Bronze | Person 1: bronze.laps, bronze.session_results, and load audit. Retain source values, batch keys, counts, and checksums. |
| Silver | Person 3: dbt models for types, units, IDs, duplicate handling, and links between the two datasets. |
| Gold | Person 3: dimensions. Person 4: facts. Use dbt source() and ref() for dependencies. |
| Serve | Person 5: six SQL queries, CSV results, and explanations linked from README. |

### Freeze the interface

- Parameters: season, round, session list, and data cutoff. Use Q and R for the required scope. Keep sprint results outside the event-summary fact.
- Join events by season and round. Map lap driver numbers to result IDs within the same event and session. Do not use display names or car numbers as permanent driver IDs.
- Record source fields, output fields, types, units, null rules, and business keys in docs/data_contract.md.
- Load full 2025 coverage and available completed 2026 events. Record the last included round and extraction date. Do not imply that 2026 is complete.


## Portions 1 and 2

Build the runtime and deliver source data

### 1. Person 1 - runtime and orchestration

| Task | Hours |
| --- | --- |
| Docker, pinned dependencies, .env.example, and database setup | 7 |
| Bronze tables, transactional loader, audit, and safe reruns | 7 |
| Parameterized DAG; ingestion tasks; dbt build; query export | 7 |
| Failure, retry, clean-start, and rerun checks | 4 |
| Setup text, DAG diagram, and report contribution | 3 |
| Review Person 2's portion and help with integration | 2 |

Outputs: compose.yaml, docker/, requirements files, dags/f1_pipeline.py, ingestion/load_bronze.py, and SQL/bronze_setup.sql.

Load to staging, then replace the selected dataset/event/session batch in a transaction. Preserve other batches. Publish only after both required inputs pass. Prevent concurrent loads of the same batch.

DAG: validate parameters -> extract both FastF1 datasets -> check files -> load bronze -> dbt build -> export queries. Share file paths between tasks; keep data files in a shared volume.

Done when: another member can start the stack, run one event twice, and recover from a failed task. An identical rerun leaves gold data and query results unchanged.

### 2. Person 2 - FastF1 extraction

| Task | Hours |
| --- | --- |
| Profile both FastF1 datasets and define the field contract | 5 |
| Build two extractors and a shared loader; save raw files and manifests | 11 |
| Handle session loading, caching, retries, and unavailable data | 5 |
| Test malformed data, duplicate keys, and missing required IDs | 4 |
| Dataset notes, dictionary inputs, and report contribution | 3 |
| Review Person 3's portion and help with integration | 2 |

Outputs: ingestion/extract_laps.py, ingestion/extract_results.py, docs/data_contract.md, dataset counts, and small labelled fixtures.

Done when: both FastF1 datasets cover matching events, preserve source values, report missing sessions, and pass contract checks. A complete load meets the row and column limits.

Handoff: supply Q and R data for one real event by 28 October. Include lap and result files, manifests, and edge-case fixtures. Share the FastF1 cache between extraction tasks.


## Portions 3 and 4

Build the dimensional model in dbt

### 3. Person 3 - silver and dimensions

| Task | Hours |
| --- | --- |
| dbt project, profiles template, sources, and dependency structure | 4 |
| Silver: types, ID mappings, units, and duplicate handling | 7 |
| Driver, team, event, and historical assignment dimensions | 10 |
| Dimension uniqueness, relationships, and history tests | 4 |
| YAML descriptions, model diagram, and report contribution | 3 |
| Review Person 4's portion and help with integration | 2 |

Outputs: dbt/dbt_project.yml, dbt/models/silver/, dbt/models/gold/dim_*.sql, YAML files, and dimension tests.

Keep Type 1 for driver, team, and event attributes. Build driver-team history from event order, not dbt run dates. Use [effective date, expiration date) intervals. These dates describe observed event assignments, not verified contract dates.

Use stable keys from natural IDs. Prefer deterministic text keys, such as driver:<source_id> and event:<season>:<round>. Document the change from integer keys. Both facts must use the same keys.

Done when: historical loads give stable keys, no overlapping assignment intervals, and at most one current assignment per driver. Fact joins resolve to one driver, team, and event.

### 4. Person 4 - facts and metrics

| Task | Hours |
| --- | --- |
| Define result status, qualifying, position, and null rules | 5 |
| Build both facts with event-batch refresh logic | 12 |
| Test grain, joins, derived values, corrections, and edge cases | 8 |
| YAML descriptions, metric notes, and report contribution | 3 |
| Review Person 5's portion and help with integration | 2 |

Outputs: dbt/models/gold/fact_*.sql, fact YAML files, dbt/tests/, and docs/metric_rules.md.

FactLap grain: one driver, lap, session, and event. FactDriverEventResult grain: one driver and main-race event, with its qualifying result. Preserve both grains.

Refresh selected event batches transactionally. Remove obsolete rows in a corrected batch. Small dimensions can rebuild from all available history. Resolve the team assignment for the event date and check it against the source team.

Done when: tests cover teammates, a team change, missing qualifying times, a pit-lane start, a retirement, and a corrected result. No fact has an unmatched dimension key.


## Portion 5 and query outputs

Turn the model into tested answers

### 5. Person 5 - queries and evidence

| Task | Hours |
| --- | --- |
| Correct six queries and define their expected outputs | 10 |
| Build the acceptance runner and result export commands | 7 |
| Check fixtures and selected source records | 5 |
| Assemble README evidence and report from team sections | 6 |
| Review Person 1's portion and help with integration | 2 |

Outputs: updated SQL/query_01...query_06, scripts/verify_pipeline.py, results/, docs/evidence/, and Reports/Report.pdf.

Person 5 assembles the report. Each member writes and checks their own technical section. Each member supplies test evidence and a useful diagram or screenshot. Final assembly is not an extra implementation role.

### Make each query answer its question

| Query | Required output and correction |
| --- | --- |
| 01 - pole wins | Season, eligible races, pole wins, and percentage. Use qualifying P1. Handle empty denominators and incomplete results. |
| 02 - team positions | Net positions by season and stable team key. Show included and excluded result counts. |
| 03 - driver positions | Net positions by season and stable driver key. Use the same eligibility rules as query 02. |
| 04 - lap-one lead | Circuit and event list, count, and retention rate. Exclude missing lap-one observations from the denominator; report them. |
| 05 - teammates | Pair drivers within an event and team. Show qualifying-position and finishing-position gaps, and head-to-head counts. Count each pair once. |
| 06 - winning gap | Winner, season, event, qualifying position, and largest non-null gap. Return ties. Show qualifying-stage comparability. |

Keep the six SQL filenames. Include season and round where event names can repeat. Save the input cutoff, query version, and row counts with each exported result.

### Acceptance of the answers

Extend the small fixture to give known answers for all six queries. It currently has no teammate pair or assignment rows. Run all queries on real pipeline data too. Explain excluded records.

Done when: six fixture answers match, real answers are reproducible, and README links to output tables. Do not present illustrative results as observed F1 results.


## Schedule and Gantt chart

20 October to 19 November 2026  |  Dates follow the course timeline

The bars show work windows, not full-time effort. Each member retains the 30-hour budget. Work can proceed in parallel after the contract is agreed.

Grey: decisions and fixtures. Teal: implementation. Blue: integration and tests. Brown: report, fixes, and release. Shading marks weekends.

| Date | Milestone and evidence |
| --- | --- |
| 20-23 October | Confirm the FastF1 field contract, scope, and owners. Profile both datasets. Freeze the extraction and join rules by 23 October. |
| 24-30 October | Start the stack. Extract and load one event. Supply silver and initial dimensions. Draft expected query results. |
| 31 Oct-6 Nov | Run one event through gold and all six queries. Complete history and facts. Finish the first end-to-end DAG run. |
| 7-11 November | Load 2025 and completed 2026 events. Check counts, reruns, corrected batches, and failed-task recovery. |
| 12-16 November | Freeze the evidence dataset. Capture results and diagrams. Assemble the report. Reproduce setup on a second machine. |
| 17-18 November | Use the buffer for defects and final review. Freeze the submission version. Recheck links and PDF layout. |
| 19 November | Person 1 submits the repository link and report by 23:59. Another member checks the upload receipt. |

Critical dependency: dataset contract -> example bronze batch -> silver and dimensions -> facts -> complete DAG -> query evidence -> final report. Use fixtures while upstream work is incomplete.

Gantt: work windows are shown in the PDF chart. The milestone table gives the shared dates.


## Rules and acceptance checks

Use the same definitions in extraction, dbt, and analytical SQL

### Agree metric rules before coding

- Pole means qualifying position 1. Keep grid position separate because penalties can change the start order.
- Positions gained equal grid position minus finishing position. Use valid numeric positions. Exclude pit-lane and unknown starts from this metric; report the excluded count.
- Retain result status. A retirement can still have an official numeric classification. State which classifications enter each query.
- Use the best valid time from the furthest qualifying stage reached, as in the proposal. Store that stage and the pole driver's stage.
- A gap across stages can reflect different track conditions. Flag it. Show a same-stage comparison beside query 06. Do not silently change the metric.
- Allow documented missing lap times and positions. Reject missing business keys. Check positive lap times only when a valid time exists.

### Prove the pipeline works

| Check | Pass condition / lead |
| --- | --- |
| Dataset profile | Both FastF1 datasets meet row and column limits without duplicates. Record Q/R coverage. / Person 2 |
| Fresh setup | A second member can start from README without hidden local files or credentials. / Person 1 |
| Ingestion failure | Bad keys or incomplete inputs fail before publication. An empty response cannot erase valid data. / Person 2 + Person 1 |
| dbt build | Dependencies resolve. Grain, not-null, relationships, validity, and history tests pass. / Person 3 + Person 4 |
| Identical rerun | Keys, counts, and content hashes match after the same batch runs twice. Exclude audit timestamps. / Person 1 |
| Corrected input | Changed and removed rows appear correctly in gold. Other events stay unchanged. / Person 4 |
| Query evidence | Six fixture answers match. Real results and exclusions are saved with their cutoff. / Person 5 |

PostgreSQL fits moderate seasonal data, dimensional joins, and event corrections. Use separate schemas, transactions, and indexes on event and join keys. Document actual row counts; do not invent performance claims.


## Submission and requirement map

Complete required work before any optional extension

### Map requirements to deliverables

| Requirement | Deliverable / owner |
| --- | --- |
| P1 brief, questions, dictionary, and SCD choices | Revised brief and model notes. Each member updates their fields. / Person 3 coordinates |
| P1 size; P2 two accepted FastF1 datasets | Separate lap/result profiles, extraction metadata, and coverage counts. / Person 2 |
| P2 Airflow, parameters, quality, and idempotency | DAG, load audit, failure checks, and rerun evidence. / Person 1 |
| P2 layers and database justification | Bronze/silver/gold schemas and design notes. / Person 1 + Person 3 |
| P2 dbt models, tests, YAML, and dependencies | Two facts, four dimensions, build logs, and dependency diagram. / Person 3 + Person 4 |
| P2 query results and runnable repository | Six result tables, setup instructions, and acceptance evidence. / Person 5 |

### Assemble the final report

Create Reports/Report.pdf with at most five pages, excluding references. Use 11-point body text. Figures and tables count. Allocate one page each to: business and changes; sources and architecture; model and dbt; pipeline checks; results and contributions.

Members supply sections and evidence by 12 November. Include roles and actual contribution percentages. The target is 20% each; report the real distribution if it changes. Add links to the AI chats used for Project 2.

README needs dependencies, environment variables, setup commands, one-event and full-load commands, rerun instructions, DAG and dbt diagrams, and six query results. Keep secrets outside Git. Preserve the Project 1 material as a traceable baseline.

### Control scope and delivery risk

If dataset counts are low, check missing sessions and extend genuine FastF1 coverage. Record any scope change. During an outage, use saved raw files with provenance. Do not replace real evidence with synthetic outputs.

Bonus work is outside the 150-hour baseline. The shared maximum is two points. Claim semi-structured integration only with source-structure handling, results, and a comparison with relational tables. A JSON export alone is not the justification.

### Sources used

[1] docs/project1-ut.pdf, all 3 pages. [2] docs/project2-ut.pdf, all 5 pages. [3] README.md and Reports/Project_long_version.md. [4] SQL/create_schema.sql, sample_data.sql, and query_01 through query_06. The source design follows the confirmed use of two FastF1 datasets.

The requirements and repository were read. No source extraction or pipeline test was performed for this plan. Hours and assignments are planning proposals.

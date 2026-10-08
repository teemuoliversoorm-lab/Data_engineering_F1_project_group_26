# F1 Analytics Data Platform

**Data Engineering Project — Group 26**
University of Tartu, III Semester

An analytical data platform that evaluates the impact of qualifying performance on race outcomes and tracks constructor performance across the 2025 and 2026 Formula 1 seasons.

For the full project proposal, including the detailed data architecture, data model, and data dictionary, see [`Reports/Project_long_version.md`](Reports/Project_long_version.md).

---

## Team Members

- Ahto Kiil
- Dagmar Läänemets
- Georg Allikas
- Kerttu Tilk
- Teemu-Oliver Soorm

---

## Project Goal

To build an analytical data platform that analyzes the relationship between qualifying performance, race outcomes, weather conditions, and constructor performance across the 2025 and 2026 Formula 1 seasons.

**Stakeholders:** F1 race team strategy analysts, sports media, fans

---

## Business Questions

1. How frequently does the pole position (P1) qualifier win the race?
2. Which teams gained or lost the most net positions across the season?
3. Which individual drivers gained or lost the most positions across the season?
4. How many times (and at which circuits) has the pole position driver maintained the lead at the end of lap 1?
5. What is the head-to-head qualifying position gap between teammates, and does it translate into final race results?
6. What is the largest qualifying time gap (and position) to pole from which a driver still managed to win the race?

---

## Key Metrics

| Metric | Description |
|---|---|
| **Position Change Delta** (Net Positions Gained/Lost) | Difference between starting grid position and final finishing position. |
| **Qualifying Gap to Pole** (Time Delta) | The gap (in seconds) between a driver's qualifying time and the P1 pole position time. |
| **Lap 1 Leader Retention Rate** | Percentage of races where the pole sitter (P1) maintains the lead at the end of Lap 1. |

---

## Data Sources

Data is extracted from the [FastF1 Python API](https://docs.fastf1.dev/api_reference/session.html), covering the 2025 and 2026 Formula 1 seasons.

| Dataset | FastF1 Location | Description |
|---|---|---|
| **Lap Timing & Track Conditions** | `session.laps` | Lap-by-lap timing and track position data collected during official Qualifying and Race sessions. |
| **Session Results & Qualifying Summary** | `session.results` | Overall session results for qualifying, sprint shootouts, and main races, capturing grid positions, qualifying time gaps, and final race classification. |

---

## Architecture Overview

```
FastF1 API → Airflow (ingestion) → Raw Landing (Docker volume, Parquet/JSON)
           → PostgreSQL (raw schema) → dbt (ELT transformation)
           → PostgreSQL (Star Schema) → Superset / Streamlit (reporting)
```

1. **Data Source:** External FastF1 Python API serving lap timing, telemetry, weather, and session results.
2. **Ingestion:** Python extraction scripts scheduled via Apache Airflow, issuing API calls following each race weekend.
3. **Raw Landing Storage:** Extracted payloads are persisted as raw Parquet/JSON files in persistent Docker volumes.
4. **Loading & Transformation (ELT):** Raw files are staged into a PostgreSQL raw schema; dbt transforms them into the analytical Star Schema.
5. **Data Quality:** Enforced via dbt test assertions (uniqueness, null checks, range/validity checks).
6. **Reporting / Serving:** The analytical schema powers Apache Superset / Streamlit dashboards and direct analytical queries.

**Update frequency:** Weekly or bi-weekly, aligned with the F1 Grand Prix calendar. Historical 2025 data is loaded via an initial bulk backfill.

---

## Tooling

| Category | Tools |
|---|---|
| Infrastructure & Containerization | Docker |
| Data Ingestion | Python (FastF1 API, Requests, Pandas), Apache Airflow |
| Data Storage (Landing) | Docker volumes |
| Transformation & Modeling | dbt, ER Diagrams / Star Schema design |
| Data Warehouse | PostgreSQL |
| Governance & Metadata | OpenMetadata |
| Reporting & Analytics | Apache Superset / Streamlit |

---

## Data Model

The warehouse implements a multi-fact star schema with two fact tables (`FactLap`, `FactDriverEventResult`) and four dimensions (`DimDriver`, `DimTeam`, `DimDriverTeamAssignment`, `DimEvent`).

See the full schema, SCD strategy, and data dictionary in [`Reports/Project_long_version.md`](Reports/Project_long_version.md#5-data-model).

---

## Repository Structure

```
.
├── README.md
└── Reports/
    ├── Project_long_version.md          # Full project proposal
```

---

## LLM Disclosure

- https://claude.ai/share/3355e7f2-f57f-496a-b356-fa8ccbd2a804
- https://chatgpt.com/share/6ac4b1cf-76fc-83ed-9fd4-a50ae037c74f
- https://chatgpt.com/share/6ac6820a-ce9c-83ed-a26d-dca92a490eb4 

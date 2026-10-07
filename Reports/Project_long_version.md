# F1 Data Engineering Project Proposal

## 1. Business Brief

**Objective:** To build an analytical data platform that evaluates the impact of qualifying performance on race outcomes and tracks constructor performance across the 2025 and 2026 Formula 1 seasons.

**Stakeholders:** F1 race team strategy analysts, sports media, fans

**Key Metrics:**
1. **Position Change Delta (Net Positions Gained/Lost):** Difference between starting grid position and final finishing position.
2. **Qualifying Gap to Pole (Time Delta):** The gap (in seconds) between a driver's qualifying time and the P1 pole position time.
3. **Lap 1 Leader Retention Rate:** Percentage of races where the pole sitter (P1) maintains the lead at the end of Lap 1.

**Business Questions:**
1. How frequently does the pole position (P1) qualifier win the race?
2. Which teams gained or lost the most net positions across the season?
3. Which individual drivers gained or lost the most positions across the season?
4. How many times (and at which circuits) has the pole position driver maintained the lead at the end of lap 1?
5. What is the head-to-head qualifying position gap between teammates, and does it translate into final race results?
6. What is the largest qualifying time gap (and position) to pole from which a driver still managed to win the race?

---

## 2. Datasets

We extract two distinct datasets from the [FastF1 Python API](https://docs.fastf1.dev/api_reference/session.html) covering the 2025 and 2026 Formula 1 seasons.

### 2.1 F1 Lap Timing & Track Conditions Dataset (high granularity)
- **Description:** Lap-by-lap timing and track position data collected during official Qualifying and Race sessions.
- **FastF1 Location:** `session.laps`
- **Key columns:** `Season`, `EventName`, `SessionType`, `Driver`, `Team`, `LapNumber`, `LapTime`, `LapPosition`, `TrackStatus`, `Time`

### 2.2 F1 Session Results & Qualifying Summary Dataset (Aggregated)
- **Description:** Overall session results for qualifying, sprint shootouts, and main races, capturing grid positions, qualifying time gaps, and final race classification.
- **FastF1 Location:** `session.results`
- **Key columns:** `Season`, `EventName`, `SessionType`, `Driver`, `Team`, `GridPosition`, `QualifyingPosition`, `FinishPosition`, `QualifyingGapToPole`, `Points`

---

## 3. Tooling

**1. Infrastructure & Containerization**
- **Docker:** Containerizes pipeline components (Airflow, PostgreSQL, dbt) to ensure consistent environment setup across local and deployment environments.

**2. Data Ingestion**
- **Python (FastF1 API / Requests / Pandas):** Extracts lap timing and session results directly from the FastF1 API.
- **Apache Airflow:** Orchestrates and schedules periodic ingestion DAGs (e.g., weekly/bi-weekly race weekend pulls).

**3. Data Storage (Landing)**
- **Local Storage (Docker volume):** Landing zone for raw API responses (JSON/CSV) prior to warehouse loading.

**4. Data Transformation & Data Modeling**
- **dbt (data build tool):** Handles the Transformation step in ELT. Performs data cleaning, enforces data quality tests, and transforms raw staging tables into target Star Schema tables.
- **ER Diagrams / Star Schema Design:** Modeling frameworks used to design relationships, foreign key constraints, and dimensional tables.

**5. Data Warehouse / OLAP Engine**
- **PostgreSQL:** Serves as the central analytical data warehouse hosting the dimensional model (Star Schema) for SQL queries.

**6. Data Governance & Metadata Management**
- **OpenMetadata:** Handles data cataloging, column-level data lineage, schema documentation, and centralized metadata management across the data stack.

**7. Reporting & Analytics (Data Serving)**
- **Apache Superset** (option 1) / **Streamlit** (option 2): Dashboards and interactive web applications for visualizing driver key metrics, lap time comparisons, and telemetry trends.

---

## 4. Data Architecture

**Pipeline Overview:**
1. **Data Source:** External FastF1 Python API serving lap timing, telemetry, weather, and session results.
2. **Ingestion:** Python extraction scripts scheduled via Apache Airflow issue API calls following each race weekend.
3. **Raw Landing Storage:** Extracted payloads are persisted as raw Parquet/JSON files in persistent Docker Volumes.
4. **Loading & Transformation (ELT):** Raw files are staged into a PostgreSQL raw schema. dbt executes SQL transformations to clean missing values, validate logic, and build the analytical Star Schema inside PostgreSQL.
5. **Data Quality Framework:** Enforced via dbt test assertions:
   - **Uniqueness Check:** Primary key composite uniqueness on `(EventKey, SessionType, DriverKey, LapNumber)` in `FactLap`.
   - **Null Value Check:** Essential metric fields (`LapTime`, `FinishPosition`, `GridPosition`) must not contain unexpected nulls.
   - **Range & Validity Check:** `LapTime > 0` and position values between 1 and 24.
   - The current validation rules don't cover all possible constraints; the list will be refined and expanded as the project develops.
6. **Reporting / Serving:** PostgreSQL analytical schema powers Apache Superset / Streamlit dashboards and direct analytical queries.

**Update Frequency:** Ingestion runs weekly or bi-weekly, aligned with the F1 Grand Prix calendar schedule. Historical 2025 data is ingested via an initial bulk backfill load.

---

## 5. Data Model

Our data warehouse implements a multi-fact star schema design to support both fine-grained telemetry/lap analytics and high-level session summary reporting.

### Fact Tables & Grains

**1. FactLap** (Grain: one row per driver, per lap, per session, per event)
- **Foreign Keys:** `EventKey`, `DriverKey`, `TeamKey`, `DriverTeamAssignmentKey`
- **Degenerate Dimensions / Attributes:** `SessionType` (qualifying, race, sprint), `LapNumber`, `LapPosition`, `TrackStatus`
- **Measures:** `LapTime` (seconds)

**2. FactDriverEventResult** (Grain: one row per driver, per event summary)
- **Foreign Keys:** `EventKey`, `DriverKey`, `TeamKey`, `DriverTeamAssignmentKey`
- **Measures:** `QualifyingPosition`, `GridPosition`, `FinishPosition`, `PlacesGainedLost`, `QualifyingGapToPole`, `Points`

### Dimension Tables & SCD Justification

| Dimension | Type | SCD Strategy & Justification |
|---|---|---|
| `DimDriver` | Type 1 | Driver identity attributes remain constant or require overwriting in case of error corrections. |
| `DimTeam` | Type 1 | Rebrands or minor administrative team metadata changes do not require preserving historical versions. |
| `DimDriverTeamAssignment` | Type 2 | Tracks driver-team affiliations over time. Includes flags to accurately attribute past performance when drivers switch teams between seasons or mid-season. |
| `DimEvent` | Type 1 | Event calendars, round numbers, and circuit metadata are fixed prior to each season, but sometimes GPs are canceled or postponed. |

---

## 6. Data Dictionary

### FactLap

| Column | Data type | Description |
|---|---|---|
| `EventKey` | int | Foreign key referencing `DimEvent` |
| `DriverKey` | int | Foreign key referencing `DimDriver` |
| `TeamKey` | int | Foreign key referencing `DimTeam` |
| `SessionType` | varchar(25) | Describes whether the lap belongs to Qualifying or Race |
| `LapNumber` | int | Indicates which lap the row is describing |
| `LapPosition` | int | Indicates which position the driver is in at the end of the lap |
| `TrackStatus` | varchar(20) | Contains the status code(s) that occurred during the lap |
| `LapTime` | decimal(8,3) | Duration of the lap in seconds |

### FactDriverEventResult

| Column | Data type | Description |
|---|---|---|
| `EventKey` | int | Foreign key referencing `DimEvent` |
| `DriverKey` | int | Foreign key referencing `DimDriver` |
| `TeamKey` | int | Foreign key referencing `DimTeam` |
| `QualifyingPosition` | int | Final position in the qualifying session |
| `QualifyingTime` | decimal(8,3) | Driver's best time from the furthest qualifying stage reached, in seconds |
| `GridPosition` | int | Official starting position of the race |
| `FinishPosition` | int | Official finishing position of the race |
| `PlacesGainedLost` | int | Derived as `GridPosition - FinishPosition` |
| `QualifyingGapToPole` | decimal(8,3) | Derived as driver qualifying time − pole qualifying time |
| `Points` | int | Championship points awarded for the result |

### DimDriver

| Column | Data type | Description |
|---|---|---|
| `DriverKey` | int | Primary key |
| `DriverId` | varchar(50) | Natural driver identifier, F1 `DriverId` |
| `DriverNumber` | int | Car number the driver uses |
| `FullName` | varchar(100) | Driver's full name |
| `Abbreviation` | char(3) | Driver's name's three-letter abbreviation |
| `CountryCode` | char(3) | Driver's country code |

### DimTeam

| Column | Data type | Description |
|---|---|---|
| `TeamKey` | int | Primary key |
| `TeamId` | varchar(50) | Natural team identifier, F1 `TeamId` |
| `TeamName` | varchar(100) | Short team name without title sponsors |

### DimDriverTeamAssignment

| Column | Data type | Description |
|---|---|---|
| `DriverTeamAssignmentKey` | int | Primary key |
| `DriverKey` | int | Driver participating in the assignment |
| `TeamKey` | int | Team represented by the driver |
| `EffectiveDate` | date | Date from which the driver is in the team |
| `ExpirationDate` | date | Date from which the driver is not in the team |
| `IsCurrent` | boolean | True if the Driver-Team relationship is currently valid |

### DimEvent

| Column | Data type | Description |
|---|---|---|
| `EventKey` | int | Primary key |
| `Season` | int | Championship year |
| `RoundNumber` | int | Championship round number |
| `EventName` | varchar(100) | Short event name without sponsors |
| `Country` | varchar(100) | Country where the event is held |
| `CircuitName` | varchar(100) | Name of the circuit |
| `StartDate` | date | Date when the event started |
| `EndDate` | date | Date when the event ended |

---

## 7. LLM Disclosure

- https://claude.ai/share/3355e7f2-f57f-496a-b356-fa8ccbd2a804
- https://chatgpt.com/share/6ac4b1cf-76fc-83ed-9fd4-a50ae037c74f
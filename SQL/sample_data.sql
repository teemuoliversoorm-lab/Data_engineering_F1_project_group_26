-- =============================================================================
-- Sample / Illustrative Data
-- =============================================================================
-- This file contains small, hand-crafted sample rows for every table in the
-- star schema (create_schema.sql). It does NOT come from the FastF1 API and
-- is NOT produced by the real ingestion pipeline (Airflow -> raw landing ->
-- dbt -> PostgreSQL). Its purpose is purely for:
--   1. Validating that create_schema.sql runs cleanly (correct DDL syntax,
--      valid FK relationships, etc.).
--   2. Providing deterministic, known inputs so the analytical queries in
--      query_01...query_06 can be test-run and their logic verified without
--      waiting on a working end-to-end pipeline.
--   3. Demos/grading, where running the full pipeline is impractical.
-- =============================================================================

INSERT INTO DimDriver (
    DriverKey,
    DriverId,
    FullName,
    Abbreviation,
    CountryCode
)
VALUES
(1, 'max_verstappen', 'Max Verstappen', 'VER', 'NED'),
(2, 'norris', 'Lando Norris', 'NOR', 'GBR');


INSERT INTO DimTeam (
    TeamKey,
    TeamId,
    TeamName
)
VALUES
(1, 'red_bull', 'Red Bull Racing'),
(2, 'mclaren', 'McLaren');


INSERT INTO DimEvent (
    EventKey,
    Season,
    RoundNumber,
    EventName,
    Country,
    CircuitName,
    StartDate,
    EndDate
)
VALUES
(
    1,
    2025,
    1,
    'Australian Grand Prix',
    'Australia',
    'Albert Park Grand Prix Circuit',
    '2025-03-14',
    '2025-03-16'
);


INSERT INTO FactLap (
    EventKey,
    DriverKey,
    TeamKey,
    SessionType,
    LapNumber,
    LapPosition,
    TrackStatus,
    LapTime
)
VALUES
(1, 2, 2, 'Race', 1, 1, '1', 82.104),
(1, 1, 1, 'Race', 1, 2, '1', 83.251),
(1, 2, 2, 'Race', 2, 1, '1', 81.987),
(1, 1, 1, 'Race', 2, 2, '1', 82.760);


INSERT INTO FactDriverEventResult (
    EventKey,
    DriverKey,
    TeamKey,
    QualifyingPosition,
    QualifyingTime,
    GridPosition,
    FinishPosition,
    PlacesGainedLost,
    QualifyingGapToPole,
    Points
)
VALUES
(1, 1, 1, 3, 75.481, 3, 2, 1, 0.385, 18),
(1, 2, 2, 1, 75.096, 1, 1, 0, 0.000, 25);

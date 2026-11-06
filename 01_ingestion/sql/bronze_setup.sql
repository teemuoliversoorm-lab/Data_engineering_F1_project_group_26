-- Airflow loads bronze only. dbt owns the silver views and gold tables (see 02_dbt/).
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA bronze IS 'Validated OpenF1 source records as JSONB, one row per driver per session.';
COMMENT ON SCHEMA silver IS 'dbt-owned typed and standardised source views.';
COMMENT ON SCHEMA gold IS 'dbt-owned Project 1 multi-fact star schema.';

CREATE TABLE IF NOT EXISTS bronze.session_results (
    season integer NOT NULL CHECK (season >= 2018),
    round_number integer NOT NULL CHECK (round_number > 0),
    session_type text NOT NULL CHECK (session_type IN ('Q', 'R')),
    driver_number text NOT NULL,
    driver_id text NOT NULL CHECK (driver_id <> ''),
    team_id text NOT NULL CHECK (team_id <> ''),
    source_record jsonb NOT NULL,
    metadata jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, round_number, session_type, driver_number),
    UNIQUE (season, round_number, session_type, driver_id)
);

CREATE TABLE IF NOT EXISTS bronze.laps (
    season integer NOT NULL,
    round_number integer NOT NULL,
    session_type text NOT NULL CHECK (session_type IN ('Q', 'R')),
    driver_number text NOT NULL,
    lap_number integer NOT NULL CHECK (lap_number > 0),
    source_record jsonb NOT NULL,
    metadata jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, round_number, session_type, driver_number, lap_number),
    FOREIGN KEY (season, round_number, session_type, driver_number)
        REFERENCES bronze.session_results (season, round_number, session_type, driver_number)
);

CREATE INDEX IF NOT EXISTS session_results_driver_idx ON bronze.session_results (driver_id, season, round_number);
CREATE INDEX IF NOT EXISTS session_results_team_idx ON bronze.session_results (team_id, season, round_number);

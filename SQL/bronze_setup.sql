-- Airflow loads bronze only. dbt owns the silver views and gold tables.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.load_audit (
    season integer NOT NULL CHECK (season >= 2018),
    round_number integer NOT NULL CHECK (round_number > 0),
    session_type text NOT NULL CHECK (session_type IN ('Q', 'R')),
    dataset text NOT NULL CHECK (dataset IN ('laps', 'session_results')),
    content_sha256 char(64) NOT NULL,
    row_count integer NOT NULL CHECK (row_count > 0),
    source_path text NOT NULL,
    metadata jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, round_number, session_type, dataset)
);

CREATE TABLE IF NOT EXISTS bronze.session_results (
    season integer NOT NULL,
    round_number integer NOT NULL,
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

CREATE INDEX IF NOT EXISTS session_results_driver_idx
    ON bronze.session_results (driver_id, season, round_number);
CREATE INDEX IF NOT EXISTS session_results_team_idx
    ON bronze.session_results (team_id, season, round_number);
COMMENT ON SCHEMA bronze IS 'Validated source records, OpenF1 raw responses and explicitly adapted records as JSONB';
COMMENT ON SCHEMA silver IS 'dbt-owned typed and standardised source views';
COMMENT ON SCHEMA gold IS 'dbt-owned Project 1 multi-fact star schema';

-- Additive migration: retained on existing warehouse volumes by init-db.
CREATE TABLE IF NOT EXISTS bronze.source_responses (
    season integer NOT NULL,
    round_number integer NOT NULL,
    session_type text NOT NULL CHECK (session_type IN ('Q', 'R')),
    source_query text NOT NULL,
    source_url text NOT NULL,
    content_sha256 char(64) NOT NULL,
    source_records jsonb NOT NULL CHECK (jsonb_typeof(source_records) = 'array'),
    metadata jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, round_number, session_type, source_query)
);
COMMENT ON TABLE bronze.source_responses IS 'Unmodified OpenF1 response records, with request URL and checksum; same transaction as both datasets';

-- A zero count records an explicit unavailable source, never a complete empty load.
ALTER TABLE bronze.load_audit DROP CONSTRAINT IF EXISTS load_audit_row_count_check;
ALTER TABLE bronze.load_audit ADD CONSTRAINT load_audit_row_count_check CHECK (
    row_count > 0 OR (
        row_count = 0 AND dataset = 'laps'
        AND COALESCE(metadata->'dataset_status'->>'laps', '') = 'unavailable'
    )
);

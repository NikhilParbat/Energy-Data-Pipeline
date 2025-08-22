-- Raw landing table (idempotent upserts use unique constraint)
CREATE TABLE IF NOT EXISTS raw_carbon_intensity (
  id BIGSERIAL PRIMARY KEY,
  region TEXT NOT NULL,                  -- e.g., 'GB'
  from_ts TIMESTAMPTZ NOT NULL,
  to_ts   TIMESTAMPTZ NOT NULL,
  forecast INT,
  actual   INT,
  intensity_index TEXT,                  -- e.g., 'low','moderate','high'
  source TEXT DEFAULT 'carbon-intensity-api',
  ingested_at TIMESTAMPTZ DEFAULT NOW(),
  CONSTRAINT uq_raw_ci UNIQUE (region, from_ts, to_ts)
);

-- Dimensions
CREATE TABLE IF NOT EXISTS region_dim (
  region_id SERIAL PRIMARY KEY,
  region_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS datetime_dim (
  ts TIMESTAMPTZ PRIMARY KEY,
  date DATE,
  hour SMALLINT,
  dow SMALLINT,                   -- day of week 0-6
  month SMALLINT,
  year INT,
  is_weekend BOOLEAN
);

-- Fact table
CREATE TABLE IF NOT EXISTS carbon_fact (
  ts TIMESTAMPTZ PRIMARY KEY REFERENCES datetime_dim(ts) ON DELETE CASCADE,
  region_id INT REFERENCES region_dim(region_id) ON DELETE RESTRICT,
  forecast INT,
  actual INT,
  intensity_index TEXT,
  last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_carbon_fact_region_ts ON carbon_fact(region_id, ts);

ALTER TABLE surveillance_case ADD COLUMN IF NOT EXISTS region VARCHAR(20) NOT NULL DEFAULT 'GLOBAL';
ALTER TABLE surveillance_case ADD COLUMN IF NOT EXISTS asset_class VARCHAR(80) NOT NULL DEFAULT 'UNCLASSIFIED';
ALTER TABLE surveillance_case ADD COLUMN IF NOT EXISTS alert_count INTEGER NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS surveillance_case_region_date_idx
  ON surveillance_case (region, created_at DESC);

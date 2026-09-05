CREATE TABLE IF NOT EXISTS ingestion_batch (
  batch_id VARCHAR(120) PRIMARY KEY,
  source_system VARCHAR(120) NOT NULL,
  schema_version VARCHAR(80) NOT NULL,
  received INTEGER NOT NULL,
  accepted INTEGER NOT NULL,
  rejected INTEGER NOT NULL,
  duplicates INTEGER NOT NULL,
  status VARCHAR(40) NOT NULL,
  result_json TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS surveillance_case (
  case_id VARCHAR(160) PRIMARY KEY,
  source_job_id VARCHAR(160),
  trade_id VARCHAR(160),
  instrument VARCHAR(160),
  typology VARCHAR(80) NOT NULL,
  risk INTEGER NOT NULL,
  confidence DOUBLE PRECISION NOT NULL,
  band VARCHAR(40) NOT NULL,
  status VARCHAR(40) NOT NULL DEFAULT 'OPEN',
  driver TEXT NOT NULL,
  evidence_refs TEXT NOT NULL,
  model_version VARCHAR(160),
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS surveillance_case_risk_idx ON surveillance_case (risk DESC);

CREATE TABLE IF NOT EXISTS audit_event (
  event_id UUID PRIMARY KEY,
  case_id VARCHAR(160) NOT NULL REFERENCES surveillance_case(case_id),
  actor_id VARCHAR(160) NOT NULL,
  actor_role VARCHAR(40) NOT NULL,
  action VARCHAR(80) NOT NULL,
  reason TEXT NOT NULL,
  from_status VARCHAR(40) NOT NULL,
  to_status VARCHAR(40) NOT NULL,
  occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS audit_event_case_idx ON audit_event (case_id, occurred_at DESC);

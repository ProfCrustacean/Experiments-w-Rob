CREATE TABLE IF NOT EXISTS pipeline_run_artifacts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id uuid NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
  artifact_key text NOT NULL,
  file_name text NOT NULL,
  mime_type text NOT NULL,
  content bytea NOT NULL,
  size_bytes integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT NOW(),
  expires_at timestamptz NOT NULL,
  CONSTRAINT uq_pipeline_run_artifacts UNIQUE (run_id, artifact_key)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_artifacts_run_id
  ON pipeline_run_artifacts(run_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_artifacts_expires_at
  ON pipeline_run_artifacts(expires_at);

CREATE TABLE IF NOT EXISTS pipeline_run_logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id uuid NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
  seq bigint NOT NULL,
  level text NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error')),
  stage text NOT NULL,
  event text NOT NULL,
  message text NOT NULL,
  payload_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT NOW(),
  expires_at timestamptz NOT NULL,
  CONSTRAINT uq_pipeline_run_logs_run_seq UNIQUE (run_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_logs_run_created
  ON pipeline_run_logs(run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_logs_expires_at
  ON pipeline_run_logs(expires_at);

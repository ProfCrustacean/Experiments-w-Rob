CREATE TABLE IF NOT EXISTS self_improvement_batches (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  requested_count integer NOT NULL CHECK (requested_count > 0),
  loop_type text NOT NULL CHECK (loop_type IN ('canary', 'full')),
  status text NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'completed_with_failures', 'failed', 'cancelled')),
  max_loops_cap integer NOT NULL DEFAULT 10 CHECK (max_loops_cap > 0),
  retry_limit integer NOT NULL DEFAULT 1 CHECK (retry_limit >= 0),
  auto_apply_policy text NOT NULL DEFAULT 'if_gate_passes' CHECK (auto_apply_policy IN ('if_gate_passes')),
  created_at timestamptz NOT NULL DEFAULT NOW(),
  started_at timestamptz,
  finished_at timestamptz,
  summary_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_self_improvement_batches_created
  ON self_improvement_batches(created_at);

CREATE INDEX IF NOT EXISTS idx_self_improvement_batches_status
  ON self_improvement_batches(status, created_at);

CREATE TABLE IF NOT EXISTS self_improvement_batch_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id uuid NOT NULL REFERENCES self_improvement_batches(id) ON DELETE CASCADE,
  sequence_no integer NOT NULL CHECK (sequence_no > 0),
  attempt_no integer NOT NULL CHECK (attempt_no > 0),
  status text NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'retried_succeeded', 'retried_failed')),
  pipeline_run_id uuid REFERENCES pipeline_runs(id) ON DELETE SET NULL,
  error_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  self_correction_context_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  gate_result_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  learning_result_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  started_at timestamptz,
  finished_at timestamptz,
  CONSTRAINT uq_self_improvement_batch_runs UNIQUE (batch_id, sequence_no, attempt_no)
);

CREATE INDEX IF NOT EXISTS idx_self_improvement_batch_runs_batch
  ON self_improvement_batch_runs(batch_id, sequence_no, attempt_no);

CREATE INDEX IF NOT EXISTS idx_self_improvement_batch_runs_status
  ON self_improvement_batch_runs(status, batch_id, sequence_no);

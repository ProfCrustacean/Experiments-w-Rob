CREATE TABLE IF NOT EXISTS self_improvement_benchmark_snapshots (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  store_id text NOT NULL,
  source_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  row_count integer NOT NULL DEFAULT 0,
  sample_size integer NOT NULL DEFAULT 0,
  dataset_hash text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_self_improvement_benchmark_snapshots_store
  ON self_improvement_benchmark_snapshots(store_id, created_at);

CREATE INDEX IF NOT EXISTS idx_self_improvement_benchmark_snapshots_hash
  ON self_improvement_benchmark_snapshots(dataset_hash);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_self_improvement_harness_runs_benchmark_snapshot'
  ) THEN
    ALTER TABLE self_improvement_harness_runs
      ADD CONSTRAINT fk_self_improvement_harness_runs_benchmark_snapshot
      FOREIGN KEY (benchmark_snapshot_id)
      REFERENCES self_improvement_benchmark_snapshots(id)
      ON DELETE SET NULL;
  END IF;
END;
$$;

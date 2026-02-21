CREATE TABLE IF NOT EXISTS self_improvement_proposals (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id uuid REFERENCES self_improvement_batches(id) ON DELETE SET NULL,
  run_id uuid REFERENCES pipeline_runs(id) ON DELETE SET NULL,
  proposal_kind text NOT NULL CHECK (
    proposal_kind IN (
      'rule_term_add',
      'rule_term_remove',
      'threshold_tune',
      'taxonomy_merge',
      'taxonomy_split',
      'taxonomy_move'
    )
  ),
  status text NOT NULL CHECK (status IN ('proposed', 'applied', 'rejected', 'rolled_back')),
  confidence_score real NOT NULL DEFAULT 0,
  expected_impact_score real NOT NULL DEFAULT 0,
  payload_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT NOW(),
  updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_self_improvement_proposals_batch_status
  ON self_improvement_proposals(batch_id, status, created_at);

CREATE INDEX IF NOT EXISTS idx_self_improvement_proposals_run
  ON self_improvement_proposals(run_id, created_at);

DROP TRIGGER IF EXISTS trg_self_improvement_proposals_updated_at ON self_improvement_proposals;
CREATE TRIGGER trg_self_improvement_proposals_updated_at
BEFORE UPDATE ON self_improvement_proposals
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS self_improvement_proposal_diffs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  proposal_id uuid NOT NULL REFERENCES self_improvement_proposals(id) ON DELETE CASCADE,
  diff_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_self_improvement_proposal_diffs_proposal
  ON self_improvement_proposal_diffs(proposal_id, created_at);

CREATE TABLE IF NOT EXISTS self_improvement_applied_changes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  proposal_id uuid NOT NULL REFERENCES self_improvement_proposals(id) ON DELETE CASCADE,
  proposal_kind text NOT NULL CHECK (
    proposal_kind IN (
      'rule_term_add',
      'rule_term_remove',
      'threshold_tune',
      'taxonomy_merge',
      'taxonomy_split',
      'taxonomy_move'
    )
  ),
  status text NOT NULL CHECK (status IN ('applied', 'rolled_back')),
  version_before text NOT NULL,
  version_after text NOT NULL,
  rollback_token text NOT NULL UNIQUE,
  metadata_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  applied_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_self_improvement_applied_changes_proposal
  ON self_improvement_applied_changes(proposal_id, applied_at);

CREATE INDEX IF NOT EXISTS idx_self_improvement_applied_changes_status
  ON self_improvement_applied_changes(status, applied_at);

CREATE TABLE IF NOT EXISTS self_improvement_rollback_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  applied_change_id uuid NOT NULL REFERENCES self_improvement_applied_changes(id) ON DELETE CASCADE,
  reason text NOT NULL,
  metadata_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_self_improvement_rollback_events_change
  ON self_improvement_rollback_events(applied_change_id, created_at);

CREATE TABLE IF NOT EXISTS self_improvement_harness_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id uuid REFERENCES self_improvement_batches(id) ON DELETE SET NULL,
  run_id uuid REFERENCES pipeline_runs(id) ON DELETE SET NULL,
  benchmark_snapshot_id uuid,
  passed boolean NOT NULL DEFAULT false,
  metric_scores_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  failed_metrics_jsonb jsonb NOT NULL DEFAULT '[]'::jsonb,
  baseline_run_id uuid REFERENCES pipeline_runs(id) ON DELETE SET NULL,
  candidate_run_id uuid REFERENCES pipeline_runs(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_self_improvement_harness_runs_batch
  ON self_improvement_harness_runs(batch_id, created_at);

CREATE INDEX IF NOT EXISTS idx_self_improvement_harness_runs_run
  ON self_improvement_harness_runs(run_id, created_at);

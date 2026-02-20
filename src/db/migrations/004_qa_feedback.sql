CREATE TABLE IF NOT EXISTS pipeline_qa_feedback (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id uuid NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
  source_sku text NOT NULL,
  predicted_category text NOT NULL,
  corrected_category text,
  corrected_attributes_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  review_status text NOT NULL CHECK (review_status IN ('pass', 'fail', 'skip')),
  review_notes text,
  imported_at timestamptz NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_pipeline_qa_feedback_run_sku UNIQUE (run_id, source_sku)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_qa_feedback_run_id ON pipeline_qa_feedback(run_id);

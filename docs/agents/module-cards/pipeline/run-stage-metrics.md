# Module Card: run-stage-metrics

Owner: pipeline-flow-owner

## Purpose

Compute run-level quality metrics and gate status from QA and enrichment data.

## When To Use

After reporting outputs are generated and before artifact generation/finalization.

## Inputs

QA rows, enrichment map, processed count.

## Outputs

Quality metric bundle (rates, counts, distributions, gate status).

## Steps

1. Compute review and acceptance metrics.\n2. Compute family-level and variant coverage metrics.\n3. Evaluate pre-QA gate status.

## Failure Signals

Metric inconsistencies versus source data, invalid rate calculations, gate result mismatch.

## Related Files

- 
- 

## Related Commands

- 
> experiments-w-rob@0.1.0 pipeline
> tsx src/cli/pipeline.ts\n- 
> experiments-w-rob@0.1.0 harness:eval
> tsx src/cli/harness-eval.ts

{
  "harnessRunId": "ca2d3af7-453d-49a9-a41c-918d9024a874",
  "storeId": "continente",
  "candidateRunId": "27255b0f-1e9a-4320-a26f-441e449e0287",
  "benchmarkSnapshot": {
    "id": "b6f6956c-3606-424c-8696-7dbc2ed5bceb",
    "storeId": "continente",
    "source": {
      "strategy": "qa_feedback_plus_hard_cases_auto_topup",
      "qa_fail_count": 0,
      "qa_pass_count": 0,
      "hard_case_count": 114,
      "recent_run_count": 14,
      "qa_reviewed_count": 0,
      "auto_topup_applied": true,
      "minimum_sample_target": 200,
      "recent_processed_count": 13778,
      "auto_topup_required_count": 86,
      "recent_needs_review_count": 9892,
      "auto_topup_synthetic_count": 0,
      "recent_auto_accepted_count": 6363,
      "auto_topup_from_recent_runs_count": 86
    },
    "rowCount": 200,
    "sampleSize": 200,
    "datasetHash": "887dadb04f2b4d754e531c600334bb54bfa0e153d3078b47b0a514878fd83c91",
    "createdAt": "2026-02-21T22:08:00.369Z"
  },
  "result": {
    "passed": false,
    "metricScores": {
      "benchmark_sample_size": 200,
      "candidate_fallback_category_rate": 0,
      "candidate_needs_review_rate": 0.36,
      "l1_delta": 0,
      "l2_delta": 0,
      "l3_delta": 0
    },
    "failedMetrics": [
      "needs_review_rate"
    ],
    "baselineRunId": "f28cc20f-4c9e-4589-90c5-cdd9d38e8439",
    "candidateRunId": "27255b0f-1e9a-4320-a26f-441e449e0287"
  }
}

## Last Verified

- 2026-02-22


# Module Card: run-support

Owner: pipeline-flow-owner

## Purpose

Shared helpers for run orchestration: sampling, provider setup, enrichment utility decisions.

## When To Use

When implementing or testing pipeline orchestration helper logic.

## Inputs

Config, provider telemetry callback, products/enrichment inputs depending on helper used.

## Outputs

Deterministic sampling, provider wrappers, enrichment improvement decisions, helper schemas.

## Steps

1. Use helper APIs from run stages.\n2. Reuse sampling/provider/enrichment utility logic consistently.\n3. Keep orchestration logic out of stage files when helperized.

## Failure Signals

Sampling split mismatch, provider initialization errors, helper contract drift across stages.

## Related Files

- 
- 

## Related Commands

- 
> experiments-w-rob@0.1.0 test
> vitest run


 RUN  v4.0.18 /Users/ian/Experiments-w-Rob

 ✓ tests/qa-report.test.ts (3 tests) 18ms
 ✓ tests/config-env.test.ts (2 tests) 156ms
 ✓ tests/run-artifacts.test.ts (2 tests) 25ms
 ✓ tests/canary-state.test.ts (3 tests) 16ms
 ✓ tests/canary-selection.test.ts (3 tests) 85ms
 ✓ tests/docs-health-check.test.ts (6 tests) 333ms
 ✓ tests/category-assignment.test.ts (10 tests) 91ms
 ✓ tests/enrichment.test.ts (5 tests) 15ms
 ✓ tests/ingest.test.ts (2 tests) 25ms
 ✓ tests/learning-apply.test.ts (2 tests) 4ms
 ✓ tests/run-logger.test.ts (4 tests) 6ms
 ✓ tests/self-improvement-orchestrator.test.ts (11 tests) 13ms
 ✓ tests/openai-telemetry.test.ts (4 tests) 10ms
 ✓ tests/category-generation.test.ts (2 tests) 6ms
 ✓ tests/canary-hotlist-parse.test.ts (2 tests) 14ms
 ✓ tests/llm-batch.test.ts (1 test) 2ms
 ✓ tests/confusion-hotlist.test.ts (1 test) 5ms
 ✓ tests/categorization.test.ts (1 test) 3ms
 ✓ tests/harness.test.ts (2 tests) 7ms
 ✓ tests/canary-gate.test.ts (4 tests) 2ms
 ✓ tests/learning-rollback.test.ts (3 tests) 2ms
 ✓ tests/run-sampling.test.ts (6 tests) 4ms
 ✓ tests/embedding.test.ts (2 tests) 3ms
 ✓ tests/self-improvement-loop-policy.test.ts (5 tests) 6ms

 Test Files  24 passed (24)
      Tests  86 passed (86)
   Start at  04:31:33
   Duration  2.05s (transform 1.63s, setup 0ms, import 4.81s, tests 851ms, environment 3ms)\n- 
> experiments-w-rob@0.1.0 pipeline
> tsx src/cli/pipeline.ts

## Last Verified

- 2026-02-22


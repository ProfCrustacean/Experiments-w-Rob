# Module Card: run-stage-metrics

Owner: pipeline-flow-owner

## Purpose

Compute run quality metrics and pre-QA gate status from QA/enrichment outputs.

## When To Use

After reporting data exists and before artifact generation/finalization.

## Inputs

- `qaRows`
- `enrichments`
- `processedCount`

## Outputs

- Counts: review, auto-accept, fallback, contradictions, attribute validation fails
- Rates: auto-accept, fallback, needs-review, validation-fail
- Family distribution and family review rates
- Variant fill rates (overall and by family)
- `preQaQualityGatePass` boolean

## Steps

1. Aggregate core counts from QA rows and enrichment records.
2. Compute operational rates and family distributions.
3. Compute variant fill-rate signals.
4. Evaluate pre-QA gate thresholds.

## Failure Signals

- Processed-count mismatches causing invalid rate denominators.
- Unexpected family distribution anomalies.
- Gate result drift compared with configured thresholds.

## Related Files

- `src/pipeline/run-stage-metrics.ts`
- `src/pipeline/run-stage-reporting.ts`
- `src/pipeline/run.ts`

## Related Commands

- `npm run pipeline`
- `npm run harness:eval`

## Last Verified

- 2026-02-22

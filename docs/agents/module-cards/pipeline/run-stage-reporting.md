# Module Card: run-stage-reporting

Owner: pipeline-flow-owner

## Purpose

Build QA report outputs and confusion-hotlist outputs for a completed run.

## When To Use

After persistence and before metrics/artifact stages.

## Inputs

- Run context: `runId`, `outputDir`, `qaSampleSize`
- `products`, `enrichments`, `assignmentsBySku`
- `logger` and `stageTimingsMs`

## Outputs

- `qaRows`
- `qaResult` (sampled rows, totals, file metadata, CSV content)
- `confusionHotlist` data and CSV file path

## Steps

1. Build QA rows from products and enrichment outcomes.
2. Write QA report CSV and log QA stage metrics.
3. Build top confusion pairs (max 20).
4. Write confusion-hotlist CSV and log completion metrics.

## Failure Signals

- QA report write failure.
- Confusion-hotlist generation/write failure.
- Output directory write permission issues.

## Related Files

- `src/pipeline/run-stage-reporting.ts`
- `src/pipeline/qa-report.ts`
- `src/pipeline/confusion-hotlist.ts`
- `src/pipeline/variant-signature.ts`

## Related Commands

- `npm run pipeline`
- `npm run qa:evaluate`
- `npm run report:download`

## Last Verified

- 2026-02-22

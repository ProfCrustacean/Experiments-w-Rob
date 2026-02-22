# Task Card: Run Catalog Pipeline

Owner: pipeline-flow-owner

## Purpose

Run one enrichment pipeline execution for a catalog input file.

## When To Use

- New catalog batch ingestion
- Re-run after taxonomy or policy updates

## Inputs

- Catalog path (`--input`)
- Store id (`--store`)
- Optional run label (`--run-label`)

## Outputs

- Pipeline run row in DB
- Artifacts (full report, QA report, confusion hotlist)
- Run logs and summary stats

## Steps

1. Confirm env values in `.env` (`DATABASE_URL`, `OPENAI_API_KEY`).
2. Run `npm run db:migrate` if schema changed.
3. Run `npm run pipeline -- --input <path> --store <store> --run-label <label>`.
4. Inspect run output in logs and generated artifacts.

## Failure Signals

- Process exits non-zero.
- Missing artifacts for run id.
- Run status marked `failed`.

## Related Files

- `src/cli/pipeline.ts`
- `src/pipeline/run.ts`
- `src/pipeline/run-stage-finalize.ts`

## Related Commands

- `npm run pipeline`
- `npm run report:list`
- `npm run report:download`

## Last Verified

- 2026-02-22


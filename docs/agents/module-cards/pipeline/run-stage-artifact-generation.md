# Module Card: run-stage-artifact-generation

Owner: pipeline-flow-owner

## Purpose

Create canonical run artifacts and summary metadata with retention timestamps.

## When To Use

After metrics are finalized and before artifact persistence.

## Inputs

- Run identity, processed data, QA/confusion CSV content
- Quality metrics, histogram/distribution summaries, taxonomy version
- OpenAI stats, attribute batch stats, stage timings
- `artifactRetentionHours`, `startedAt`, `logger`

## Outputs

- `artifacts` payload list
- `artifactSummaries`
- `artifactsExpireAt`

## Steps

1. Compute run finish timestamp and artifact expiry timestamp.
2. Build full artifact payloads and summaries via `buildRunArtifacts(...)`.
3. Log artifact generation stage duration and artifact count.

## Failure Signals

- Artifact payload build errors.
- Missing required artifact summary fields.
- Unexpected artifact count or malformed CSV payloads.

## Related Files

- `src/pipeline/run-stage-artifact-generation.ts`
- `src/pipeline/run-artifacts.ts`
- `src/pipeline/run.ts`

## Related Commands

- `npm run pipeline`
- `npm run report:list`

## Last Verified

- 2026-02-22

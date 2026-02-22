# Module Card: run-stage-artifact-persist

Owner: pipeline-flow-owner

## Purpose

Persist run artifacts (disk + DB) and execute retention cleanup for artifacts and traces.

## When To Use

After artifact generation and before run finalization.

## Inputs

- `runId`, `outputDir`
- `artifacts` and `artifactsExpireAt`
- Retention controls: `artifactRetentionHours`, `traceRetentionHours`
- `logger` and `stageTimingsMs`

## Outputs

- Persisted artifact files and DB records
- `cleanedArtifacts`
- `cleanedLogs`

## Steps

1. Persist each artifact to local output directory.
2. Upsert each artifact payload into run artifact storage.
3. Run expired-artifact cleanup and expired-log cleanup.
4. Log cleanup deletion counts.

## Failure Signals

- File write failures in output directory.
- Artifact upsert failures.
- Cleanup queries failing or timing out.

## Related Files

- `src/pipeline/run-stage-artifact-persist.ts`
- `src/pipeline/persist.ts`
- `src/pipeline/run.ts`

## Related Commands

- `npm run pipeline`
- `npm run logs:download`

## Last Verified

- 2026-02-22

# Module Card: run-stage-finalize

Owner: pipeline-flow-owner

## Purpose

Finalize pipeline runs for success or failure, persist terminal stats, and return final summary output.

## When To Use

At the end of orchestration in both completed and failed paths.

## Inputs

- Completed-run context (quality metrics, artifacts, QA outputs, timings)
- Or failed-run context (error, timings, sampling context)
- `logger` and run identifiers

## Outputs

- Completed path: `PipelineRunSummary` and persisted completed status
- Failed path: persisted failed status and error details

## Steps

1. Completed path logs `run.completed`, flushes traces, and persists full stats payload.
2. Failed path logs `run.failed`, flushes traces, and persists failure payload.
3. Emit final console summary for completed runs.

## Failure Signals

- `finalizePipelineRun(...)` persistence failure.
- Trace flush failures before final persist.
- Completed/failed status mismatch in persisted run state.

## Related Files

- `src/pipeline/run-stage-finalize.ts`
- `src/pipeline/persist.ts`
- `src/pipeline/run.ts`

## Related Commands

- `npm run pipeline`
- `npm run logs:run`

## Last Verified

- 2026-02-22

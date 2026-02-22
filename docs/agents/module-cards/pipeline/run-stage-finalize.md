# Module Card: run-stage-finalize

Owner: pipeline-flow-owner

## Purpose

Finalize run state for success/failure, flush logs, and publish run summary stats.

## When To Use

At end of pipeline execution for both success and failure paths.

## Inputs

Run metadata, quality metrics, artifact summaries, trace stats, error state (if any).

## Outputs

Finalized pipeline run status and summary payload.

## Steps

1. Log terminal run event.\n2. Flush run logger trace batches.\n3. Persist final run status and stats payload.\n4. Return final summary shape.

## Failure Signals

Finalize persistence failure, trace flush errors, mismatch between run state and summary payload.

## Related Files

- 
- 

## Related Commands

- 
> experiments-w-rob@0.1.0 pipeline
> tsx src/cli/pipeline.ts\n- 
> experiments-w-rob@0.1.0 logs:run
> tsx src/cli/logs-run.ts

## Last Verified

- 2026-02-22


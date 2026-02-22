# Module Card: run-stage-artifact-persist

Owner: pipeline-flow-owner

## Purpose

Persist generated artifacts and perform artifact/log cleanup.

## When To Use

After artifact payload generation succeeds.

## Inputs

Run id, output dir, artifact payload list, retention settings, logger.

## Outputs

Persisted local files and DB artifacts, cleanup counts for artifacts and logs.

## Steps

1. Persist artifact files locally and in DB.\n2. Run retention cleanup for artifacts and logs.\n3. Return cleanup counts.

## Failure Signals

Artifact persistence write error, cleanup query failures, retention mismatch behavior.

## Related Files

- 
- 

## Related Commands

- 
> experiments-w-rob@0.1.0 pipeline
> tsx src/cli/pipeline.ts\n- 
> experiments-w-rob@0.1.0 logs:download
> tsx src/cli/logs-download.ts

## Last Verified

- 2026-02-22


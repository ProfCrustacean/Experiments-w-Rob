# Module Card: self-improvement-orchestrator

Owner: self-improvement-flow-owner

## Purpose

Coordinate queue claim, loop execution, and batch finalization.

## When To Use

Inside worker batch processing cycles.

## Inputs

Queued batch, policy settings, persistence APIs.

## Outputs

Completed/failed/cancelled batch status updates.

## Steps

1. Claim batch.
2. Execute loops in sequence.
3. Persist summary and status.

## Failure Signals

Stuck running batches or finalization mismatches.

## Related Files

- src/pipeline/self-improvement-orchestrator.ts
- src/pipeline/run.ts

## Related Commands

- npm run self-improve:worker
- npm run self-improve:status

## Last Verified

- 2026-02-22


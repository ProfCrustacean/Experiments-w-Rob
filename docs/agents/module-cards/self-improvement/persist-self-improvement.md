# Module Card: persist-self-improvement

Owner: self-improvement-flow-owner

## Purpose

Persistence layer for batches, attempts, proposals, applies, and rollbacks.

## When To Use

Whenever self-improvement entities are read or updated.

## Inputs

Entity payloads and query options.

## Outputs

Normalized DB rows and persistence acknowledgments.

## Steps

1. Execute persistence operations.
2. Enforce expected status transitions.

## Failure Signals

DB query failure or invalid status transitions.

## Related Files

- src/pipeline/persist-self-improvement.ts
- src/pipeline/run.ts

## Related Commands

- npm run self-improve:worker
- npm run self-improve:status

## Last Verified

- 2026-02-22


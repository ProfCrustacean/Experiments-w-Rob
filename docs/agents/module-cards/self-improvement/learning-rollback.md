# Module Card: learning-rollback

Owner: self-improvement-flow-owner

## Purpose

Rollback applied changes when degradation is detected or requested.

## When To Use

During post-apply watch failures or manual rollback operations.

## Inputs

Applied change id or latest selector.

## Outputs

Rollback event and reverted state metadata.

## Steps

1. Resolve target.
2. Revert patch.
3. Persist rollback event.

## Failure Signals

Rollback target not found or reversion conflict.

## Related Files

- src/pipeline/learning-rollback.ts
- src/pipeline/run.ts

## Related Commands

- npm run learn:rollback
- npm run self-improve:worker

## Last Verified

- 2026-02-22


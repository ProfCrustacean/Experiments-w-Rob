# Task Card: Rollback Learning Change

Owner: self-improvement-flow-owner

## Purpose

Revert an applied learning change when post-apply quality degrades.

## When To Use

- Watch-loop degradation is detected
- Manual rollback is requested by operator

## Inputs

- Latest applied change id or explicit change id

## Outputs

- Rollback event record
- Reverted patch state in taxonomy/rules

## Steps

1. Review latest applied change context.
2. Run rollback command:
   - Latest: `npm run learn:rollback -- --latest`
   - Specific: `npm run learn:rollback -- --applied-change-id <id>`
3. Re-run canary/harness checks.

## Failure Signals

- Rollback command exits non-zero.
- Applied change remains active after rollback.
- Post-rollback quality still degraded.

## Related Files

- `src/cli/learn-rollback.ts`
- `src/pipeline/learning-rollback.ts`
- `src/pipeline/rule-patch.ts`

## Related Commands

- `npm run learn:rollback`
- `npm run canary`
- `npm run harness:eval`

## Last Verified

- 2026-02-22


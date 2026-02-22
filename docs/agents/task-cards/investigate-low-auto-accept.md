# Task Card: Investigate Low Auto-Accept

Owner: quality-tooling-owner

## Purpose

Diagnose why `auto_accepted_rate` falls below target and identify next actions.

## When To Use

- Scoreboard shows low canary auto-accept
- Harness gate regresses unexpectedly

## Inputs

- Current quality scoreboard
- Recent run artifacts and confusion hotlists

## Outputs

- Root-cause shortlist
- Action proposal (rule tuning, threshold change, rollback, or no-op)

## Steps

1. Open `docs/quality-scoreboard.md` and confirm current rate.
2. Identify recent canary run ids.
3. Review QA and confusion artifacts for those runs.
4. Check fallback and needs-review rates together with auto-accept.
5. Propose corrective action and re-run canary.

## Failure Signals

- No reproducible signal in artifacts.
- Metric movement conflicts across runs.
- Proposed action cannot be validated by canary/harness.

## Related Files

- `docs/quality-scoreboard.md`
- `src/pipeline/run-stage-metrics.ts`
- `src/pipeline/confusion-hotlist.ts`

## Related Commands

- `npm run report:list`
- `npm run report:download`
- `npm run canary`

## Last Verified

- 2026-02-22


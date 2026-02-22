# Task Card: Run Canary

Owner: pipeline-flow-owner

## Purpose

Run a deterministic canary subset and evaluate quality gate behavior.

## When To Use

- Before broader rollout
- After learning changes or threshold updates

## Inputs

- Env-based store and input settings
- Canary threshold configuration

## Outputs

- Canary run id
- Canary subset file and state updates
- Gate pass/fail evaluation metrics

## Steps

1. Ensure `CATALOG_INPUT_PATH` and `STORE_ID` are set.
2. Run `npm run canary`.
3. Check resulting gate output and run stats.
4. Compare `auto_accepted_rate` to configured threshold.

## Failure Signals

- Canary command exits non-zero.
- Canary subset file not generated.
- Gate metrics missing from run stats.

## Related Files

- `src/cli/canary-run.ts`
- `src/canary/select-subset.ts`
- `src/canary/gate.ts`

## Related Commands

- `npm run canary`
- `npm run report:list`

## Last Verified

- 2026-02-22


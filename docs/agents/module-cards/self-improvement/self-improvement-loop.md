# Module Card: self-improvement-loop

Owner: self-improvement-flow-owner

## Purpose

Run one self-improvement loop including gates and apply policies.

## When To Use

During each orchestrator loop attempt.

## Inputs

Loop context, policy thresholds, run and learning services.

## Outputs

Loop result with gate and apply outcome.

## Steps

1. Execute run.
2. Evaluate gates.
3. Generate/apply proposals when allowed.

## Failure Signals

Loop runtime failure or policy enforcement mismatch.

## Related Files

- src/pipeline/self-improvement-loop.ts
- src/pipeline/run.ts

## Related Commands

- npm run self-improve:worker
- npm run canary

## Last Verified

- 2026-02-22


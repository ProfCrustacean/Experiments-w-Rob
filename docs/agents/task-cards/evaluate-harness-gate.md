# Task Card: Evaluate Harness Gate

Owner: quality-tooling-owner

## Purpose

Evaluate candidate run quality against harness thresholds.

## When To Use

- Before applying learning proposals
- During CI gate verification

## Inputs

- Store id
- Optional candidate run id
- Existing benchmark snapshot

## Outputs

- Harness pass/fail decision
- Delta and guardrail metrics

## Steps

1. Build benchmark if needed: `npm run harness:build -- --store <store>`.
2. Evaluate run: `npm run harness:eval -- --store <store> --candidate-run-id <runId>`.
3. Review output metrics and threshold comparisons.

## Failure Signals

- Harness command exits non-zero.
- Benchmark refresh fails.
- Missing threshold stats in output.

## Related Files

- `src/cli/harness-build.ts`
- `src/cli/harness-eval.ts`
- `src/pipeline/harness.ts`

## Related Commands

- `npm run harness:build`
- `npm run harness:eval`

## Last Verified

- 2026-02-22


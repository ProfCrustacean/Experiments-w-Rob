# Module Card: learning-proposal-generator

Owner: self-improvement-flow-owner

## Purpose

Generate ranked learning proposals from loop evidence.

## When To Use

When loop output indicates changes are needed.

## Inputs

Run metrics, confusion signals, policy filters.

## Outputs

Proposal list with confidence and metadata.

## Steps

1. Build learning context.
2. Generate candidates.
3. Rank and persist proposals.

## Failure Signals

No viable proposals or malformed proposal payloads.

## Related Files

- src/pipeline/learning-proposal-generator.ts
- src/pipeline/run.ts

## Related Commands

- npm run learn:propose
- npm run self-improve:worker

## Last Verified

- 2026-02-22


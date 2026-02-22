# Task Card: Run Self-Improve Worker

Owner: self-improvement-flow-owner

## Purpose

Process queued self-improvement batches with retry and apply policy controls.

## When To Use

- Continuous autonomous loop processing
- One-off queue drain or debug run (`--once`)

## Inputs

- Queued batch records
- Self-improvement policy env settings

## Outputs

- Updated batch and attempt statuses
- Loop summaries and apply decisions
- Optional rollback actions

## Steps

1. Start worker: `npm run self-improve:worker`.
2. Enqueue a batch: `npm run self-improve:enqueue -- --phrase "run 3 self-improvement canary loops"`.
3. Monitor: `npm run self-improve:status -- --phrase "show self-improvement batches"`.
4. Inspect a batch by id when needed.

## Failure Signals

- Worker loop crashes repeatedly.
- Batches remain stuck in `running` without progress.
- Retry count exceeds configured limit unexpectedly.

## Related Files

- `src/cli/self-improve-worker.ts`
- `src/pipeline/self-improvement-orchestrator.ts`
- `src/pipeline/self-improvement-loop.ts`

## Related Commands

- `npm run self-improve:worker`
- `npm run self-improve:status`
- `npm run self-improve:cancel`

## Last Verified

- 2026-02-22


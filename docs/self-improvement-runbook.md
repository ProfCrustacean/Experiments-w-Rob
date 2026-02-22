# Self-Improvement Runbook

Operator-focused entrypoint for running and troubleshooting self-improvement batches.

## Core Workflow

1. Start worker: `npm run self-improve:worker`
2. Enqueue loop batch: `npm run self-improve:enqueue -- --phrase "run 3 self-improvement canary loops"`
3. Check status: `npm run self-improve:status -- --phrase "show self-improvement batches"`
4. Investigate and apply/rollback as needed.

## Natural Language Contracts

- `run <N> self-improvement canary loops`
- `run <N> self-improvement full loops`
- `show self-improvement batches`
- `show self-improvement batch <batchId>`

## Operational Guardrails

- Retry bound: one retry for runtime/system failure loops.
- Tiered canary apply: full / partial low-risk / no-apply.
- Structural change cap per loop.
- Post-apply watch and optional rollback on degrade.

## Detailed Docs

- Self-improvement map: `docs/agents/maps/self-improvement-map.md`
- Worker task card: `docs/agents/task-cards/run-self-improve-worker.md`
- Rollback task card: `docs/agents/task-cards/rollback-learning-change.md`
- Reliability policy: `docs/reliability-standards.md`


# Reliability Standards

## Goal

Ensure learning changes improve quality safely and can be reverted quickly.

## Operational standards

- Loop mode: queued async worker.
- Max loops per request: 10.
- Retry policy: one retry per failed loop.
- Auto-apply policy: only when gate/harness checks pass.
- Structural apply cap: max 2 structural changes per loop.
- Rollback watch window: next 2 loops after apply.

## Gate thresholds (defaults)

- `HARNESS_MIN_L1_DELTA=0`
- `HARNESS_MIN_L2_DELTA=0`
- `HARNESS_MIN_L3_DELTA=0`
- `HARNESS_MAX_FALLBACK_RATE=0.06`
- `HARNESS_MAX_NEEDS_REVIEW_RATE=0.35`
- `SELF_IMPROVE_GATE_MIN_SAMPLE_SIZE=200`

## Failure handling expectations

- Single loop failure must not stop the batch.
- Retry once, then continue with next sequence.
- Batch fails only for worker-level fatal errors.
- Post-apply degrade should trigger rollback when enabled.

## Documentation reliability requirements

- All documented commands must exist in `package.json` scripts.
- All documented local file paths must resolve.
- Core runbook docs must exist and be linked from README.
- Docs checks run on PRs and on daily gardening schedule.
- `docs/quality-scoreboard.md` must be refreshed at least every 48 hours.

## Documentation SLA and escalation

- SLA: fix docs-check errors within 24 hours.
- SLA: reduce docs-check warnings within 3 business days.
- Escalation path:
  - docs-garden opens a debt issue automatically when checks fail
  - optional repo variable `DOCS_OWNER` auto-assigns the issue
  - workflow failure blocks “healthy” status until fixed

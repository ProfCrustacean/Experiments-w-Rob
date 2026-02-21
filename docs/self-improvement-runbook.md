# Self-Improvement Runbook

This runbook is for operating the self-improvement loop with minimal engineering overhead.

## What this system does

A self-improvement batch runs multiple loop attempts (`canary` or `full`), evaluates quality gates, proposes changes, and only auto-applies safe changes when gates pass.

Each loop retries once only for runtime/system failures.
Gate failures are terminal for that loop (no retry).
On canary retries, degrade mode is enabled automatically (higher proposal-confidence floor and no structural proposals).
If a worker is interrupted, stale `running` attempts are auto-recovered and the batch is requeued automatically.
The worker also recovers stale `pipeline_runs` rows for the configured store, so interrupted runs do not stay stuck in `running`.
Harness benchmarking is auto-maintained: undersized snapshots are rebuilt and topped up automatically to the configured minimum sample target.

## Fast start

1. Start the worker in one terminal:

```bash
npm run self-improve:worker
```

2. Enqueue a batch from another terminal:

```bash
npm run self-improve:enqueue -- --phrase "run 3 self-improvement canary loops"
```

3. Check progress:

```bash
npm run self-improve:status -- --phrase "show self-improvement batches"
```

4. Inspect one batch:

```bash
npm run self-improve:status -- --phrase "show self-improvement batch <batchId>"
```

## Supported natural-language trigger phrases

Use these exact patterns:

- `run <N> self-improvement canary loops`
- `run <N> self-improvement full loops`
- `run <N> self-improvement canary loop`
- `run <N> self-improvement full loop`
- `show self-improvement batches`
- `show self-improvement batch <batchId>`

Rules:

- `N` must be an integer from `1` to `10`
- Requests above `10` are rejected
- Ambiguous phrases are rejected with a clear correction hint

## What status fields mean

Main fields returned by `self-improve:status`:

- `status`: current batch state (`queued`, `running`, `completed`, `completed_with_failures`, `failed`, `cancelled`)
- `totalLoops`: loops requested in this batch
- `completedLoops`: loops finished (success or fail)
- `failedLoops`: loops that still failed after retry
- `currentlyRunningLoop`: sequence number of the active loop
- `lastFailureReason`: latest failure summary
- `retryAttempted`: whether retry was used on the latest failure
- `anyUpdatesAutoApplied`: whether any updates were auto-applied
- `autoAppliedUpdatesCount`: count of applied updates in the batch
- `gatePassRate`: pass ratio across completed loops

## Safety and learning policy

Current default policy:

- Retry only runtime/system failures (single retry limit)
- Auto-apply only when gates pass
- Max `2` structural changes per loop
- Canary retry degrade mode: structural proposals disabled and higher proposal-confidence floor
- Monitor next `2` loops after apply
- Roll back if post-apply quality degrades (when enabled)

Config keys controlling this:

- `SELF_IMPROVE_MAX_LOOPS`
- `SELF_IMPROVE_RETRY_LIMIT`
- `SELF_IMPROVE_AUTO_APPLY_POLICY`
- `SELF_IMPROVE_WORKER_POLL_MS`
- `SELF_IMPROVE_STALE_RUN_TIMEOUT_MINUTES`
- `STALE_RUN_TIMEOUT_MINUTES`
- `PRODUCT_PERSIST_STAGE_TIMEOUT_MS`
- `PRODUCT_VECTOR_QUERY_TIMEOUT_MS`
- `PRODUCT_VECTOR_BATCH_SIZE`
- `SELF_IMPROVE_MAX_STRUCTURAL_CHANGES_PER_LOOP`
- `SELF_IMPROVE_POST_APPLY_WATCH_LOOPS`
- `SELF_IMPROVE_ROLLBACK_ON_DEGRADE`
- `SELF_IMPROVE_CANARY_RETRY_DEGRADE_ENABLED`
- `SELF_IMPROVE_CANARY_RETRY_MIN_PROPOSAL_CONFIDENCE`

## Manual controls

Cancel a batch:

```bash
npm run self-improve:cancel -- --batch-id <batchId>
```

Manual rollback of latest applied change:

```bash
npm run learn:rollback -- --latest
```

Manual rollback by id:

```bash
npm run learn:rollback -- --applied-change-id <appliedChangeId>
```

## CI gate setup

Workflow: `.github/workflows/harness-gate.yml`

Required GitHub settings:

- Secret: `DATABASE_URL`
- Variable: `STORE_ID`
- Optional variable: `HARNESS_CANDIDATE_RUN_ID`

# Self-Improvement Runbook

This runbook is for operating the self-improvement loop with minimal engineering overhead.

## What this system does

A self-improvement batch runs multiple loop attempts (`canary` or `full`), evaluates quality gates, proposes changes, and only auto-applies safe changes when gates pass.

Each loop can retry once if it fails.

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

- Auto-apply only when gates pass
- Max `2` structural changes per loop
- Monitor next `2` loops after apply
- Roll back if post-apply quality degrades (when enabled)

Config keys controlling this:

- `SELF_IMPROVE_MAX_LOOPS`
- `SELF_IMPROVE_RETRY_LIMIT`
- `SELF_IMPROVE_AUTO_APPLY_POLICY`
- `SELF_IMPROVE_MAX_STRUCTURAL_CHANGES_PER_LOOP`
- `SELF_IMPROVE_POST_APPLY_WATCH_LOOPS`
- `SELF_IMPROVE_ROLLBACK_ON_DEGRADE`

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


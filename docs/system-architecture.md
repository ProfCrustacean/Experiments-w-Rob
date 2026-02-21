# System Architecture

## Purpose

This system ingests product catalogs, assigns categories and attributes, and continuously improves classification quality using a gated self-improvement loop.

## Primary runtime flows

## Flow 1: Catalog pipeline

1. Input is loaded from CSV/XLSX.
2. Products are normalized and enriched.
3. Category and attribute predictions are persisted.
4. QA/confusion artifacts are generated.

Main modules:

- `src/pipeline/run.ts`
- `src/pipeline/category-assignment.ts`
- `src/pipeline/enrichment.ts`
- `src/pipeline/persist.ts`

## Flow 2: Self-improvement pipeline

1. Operator enqueues a batch (`canary` or `full`).
2. Worker processes loops sequentially.
3. Each loop runs harness and gate checks.
4. Learning proposals are generated.
5. Proposals are auto-applied only if gates pass.
6. Post-apply watch can rollback on degrade.

Main modules:

- `src/pipeline/self-improvement-orchestrator.ts`
- `src/pipeline/self-improvement-loop.ts`
- `src/pipeline/self-improvement-phrase.ts`
- `src/pipeline/harness.ts`
- `src/pipeline/learning-proposal-generator.ts`
- `src/pipeline/learning-apply.ts`
- `src/pipeline/learning-rollback.ts`
- `src/pipeline/persist-self-improvement.ts`
- `src/pipeline/rule-patch.ts`

## Persistence boundaries

- General pipeline persistence: `src/pipeline/persist.ts`
- Self-improvement and learning persistence: `src/pipeline/persist-self-improvement.ts`

## Design invariants

- No auto-apply without passing gate/harness checks.
- Failed loop retries are bounded (`SELF_IMPROVE_RETRY_LIMIT`).
- Batch loop count is bounded (`SELF_IMPROVE_MAX_LOOPS`, max 10).
- Applied learning changes are auditable and rollbackable.

## CI enforcement points

- Harness quality gate in `.github/workflows/harness-gate.yml`
- Documentation health gate in `.github/workflows/docs-gate.yml`
- Scheduled docs gardening in `.github/workflows/docs-garden.yml`

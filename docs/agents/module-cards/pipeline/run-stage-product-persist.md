# Module Card: run-stage-product-persist

Owner: pipeline-flow-owner

## Purpose

Persist enriched products and vectors with explicit product and vector upsert substages.

## When To Use

After embeddings are available and before reporting/artifact stages.

## Inputs

- Identity and run context: `storeId`, `runId`
- `products`, `enrichments`, `categoriesBySlug`
- `vectorsBySku`, `embeddedTextBySku`, `embeddingModel`
- Timeouts and batch controls: `queryTimeoutMs`, `vectorBatchSize`, `persistStageTimeoutMs`
- `logger` and `stageTimingsMs`

## Outputs

- `persistedProductCount`
- `vectorPersistedRows`
- `vectorPersistedBatches`

## Steps

1. Start product-persist stage and run products-upsert substage.
2. Upsert products in batches and record progress logs.
3. Run vectors-upsert substage with configured timeout and batch size.
4. Persist vectors, capture batch-level progress, and log stage totals.

## Failure Signals

- Product or vector upsert timeout.
- Query timeout while persisting products or vectors.
- Persisted counts below expected processed rows.

## Related Files

- `src/pipeline/run-stage-product-persist.ts`
- `src/pipeline/persist.ts`
- `src/pipeline/run.ts`

## Related Commands

- `npm run pipeline`
- `npm run logs:run`

## Last Verified

- 2026-02-22

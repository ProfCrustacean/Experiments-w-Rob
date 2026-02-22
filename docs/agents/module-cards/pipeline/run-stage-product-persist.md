# Module Card: run-stage-product-persist

Owner: pipeline-flow-owner

## Purpose

Persist enriched products and vector rows with substage-level progress and timeouts.

## When To Use

After embedding vectors are ready.

## Inputs

Store id, run id, products, enrichments, categories, vectors, query/timeout settings.

## Outputs

Persisted product count, persisted vector row count, persisted vector batch count.

## Steps

1. Upsert product records in batches.\n2. Upsert vector records in batches.\n3. Log substage and stage completion metrics.

## Failure Signals

DB timeout in products or vectors substage, partial persistence, repeated substage failures.

## Related Files

- 
- 

## Related Commands

- 
> experiments-w-rob@0.1.0 pipeline
> tsx src/cli/pipeline.ts

## Last Verified

- 2026-02-22


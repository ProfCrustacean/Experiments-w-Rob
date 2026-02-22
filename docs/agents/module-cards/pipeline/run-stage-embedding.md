# Module Card: run-stage-embedding

Owner: pipeline-flow-owner

## Purpose

Create embedding text payloads and request vectors for enriched products.

## When To Use

After enrichment and before product/vector persistence.

## Inputs

Products, enrichment map, category map, embedding provider, batch/concurrency settings.

## Outputs

Vectors by SKU and embedding text by SKU.

## Steps

1. Build embedding text per product.\n2. Batch embedding requests with configured concurrency.\n3. Normalize vectors to target dimensions.\n4. Return vectors and embedded text maps.

## Failure Signals

Embedding provider timeout/retry exhaustion, vector dimension mismatch before normalization.

## Related Files

- 
- 

## Related Commands

- 
> experiments-w-rob@0.1.0 pipeline
> tsx src/cli/pipeline.ts

## Last Verified

- 2026-02-22


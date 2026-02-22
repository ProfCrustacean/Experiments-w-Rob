# Module Card: run-stage-embedding

Owner: pipeline-flow-owner

## Purpose

Generate normalized embedding vectors and embedding text payloads for persisted products.

## When To Use

After categorization/enrichment and before product vector persistence.

## Inputs

- `products`
- `enrichments`
- `categoriesBySlug`
- `embeddingProvider`
- `embeddingBatchSize` and `embeddingConcurrency`
- `logger` and `stageTimingsMs`

## Outputs

- `vectorsBySku` (dimension-normalized vectors)
- `embeddedTextBySku` (text used for embedding)

## Steps

1. Build embedding text for each product using category and attribute context.
2. Call batched embedding generation with configured concurrency.
3. Normalize vectors to `EMBEDDING_DIMENSIONS`.
4. Log stage completion and embedded product count.

## Failure Signals

- Embedding provider errors or retries exhausted.
- Missing enrichment context resulting in reduced vector coverage.
- Unexpected vector dimension mismatches.

## Related Files

- `src/pipeline/run-stage-embedding.ts`
- `src/pipeline/embedding.ts`
- `src/pipeline/run-support.ts`

## Related Commands

- `npm run pipeline`
- `npm run canary`

## Last Verified

- 2026-02-22

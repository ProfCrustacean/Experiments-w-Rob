# Module Card: run-stage-categorize-enrich

Owner: pipeline-flow-owner

## Purpose

Assign categories, persist category drafts, and build product enrichments (first pass plus optional second pass).

## When To Use

After ingest and startup, before embedding and persistence.

## Inputs

- `storeId` and sampled `products`
- Providers: `embeddingProvider`, `llmProvider`
- `usingOpenAI` flag
- Config thresholds and batch settings from `AppConfig`
- `logger` and mutable `stageTimingsMs`

## Outputs

- `categoryAssignments`
- `categoriesBySlug` and `categoryCount`
- `taxonomyVersion`
- `enrichmentMap`
- `enrichmentStats` counters for first and second pass

## Steps

1. Run category assignment and capture confidence histogram.
2. Build taxonomy-aligned category drafts and persist them.
3. Build rule-based enrichments and group LLM attribute batches by category/schema.
4. Execute first-pass attribute extraction with retries/fallback handling.
5. Optionally run second-pass escalation batches and apply only improved enrichments.
6. Backfill any missing enrichment entries and log final enrichment-stage metrics.

## Failure Signals

- Category draft persistence fails.
- Attribute batch requests fail repeatedly.
- High fallback enrichment counts or missing category coverage.
- Stage timing spikes tied to second-pass escalation.

## Related Files

- `src/pipeline/run-stage-categorize-enrich.ts`
- `src/pipeline/category-assignment.ts`
- `src/pipeline/enrichment.ts`
- `src/pipeline/persist.ts`
- `src/pipeline/run-support.ts`

## Related Commands

- `npm run pipeline`
- `npm run canary`

## Last Verified

- 2026-02-22

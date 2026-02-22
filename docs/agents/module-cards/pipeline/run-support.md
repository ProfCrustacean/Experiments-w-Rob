# Module Card: run-support

Owner: pipeline-flow-owner

## Purpose

Provide shared helper logic used across pipeline stages (sampling, provider setup, enrichment utility decisions).

## When To Use

When stage code needs reusable orchestration helpers instead of duplicating logic.

## Inputs

- Sampling inputs (`products`, `storeId`, `sampleParts`, `samplePartIndex`)
- Provider setup inputs (runtime config and optional OpenAI telemetry callback)
- Enrichment inputs (`CategoryContext`, `ProductEnrichment`, thresholds)
- Vector and progress utility inputs

## Outputs

- Deterministic sampling partitions
- Provider bundle (OpenAI or fallback)
- Escalation schemas and enrichment-improvement decisions
- Vector normalization and progress utility behavior
- Fallback enrichment payloads for missing category context

## Steps

1. Validate sampling parameters and partition products deterministically.
2. Create providers based on API-key availability.
3. Build escalation schemas from uncertainty signals and policy thresholds.
4. Compare candidate enrichments and apply improvement rules.
5. Expose utility functions for vector sizing and progress logging.

## Failure Signals

- Invalid sampling configuration throws.
- Provider creation fails due to missing/invalid config.
- Escalation schema drift causing under/over-escalation.
- Utility misuse causing inconsistent vector dimensions.

## Related Files

- `src/pipeline/run-support.ts`
- `src/pipeline/run-stage-ingest.ts`
- `src/pipeline/run-stage-categorize-enrich.ts`
- `src/pipeline/run-stage-embedding.ts`

## Related Commands

- `npm run pipeline`
- `npm run pipeline:env`
- `npm run test`

## Last Verified

- 2026-02-22

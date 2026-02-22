# Module Card: run-stage-ingest

Owner: pipeline-flow-owner

## Purpose

Convert raw catalog input into a validated sampled product set for downstream stages.

## When To Use

At the beginning of every run, before startup/categorization and before any OpenAI calls.

## Inputs

- `inputPath` to the source catalog file.
- `storeId` used for deterministic sampling partition.
- Sampling controls: `sampleParts` and `samplePartIndex`.

## Outputs

- `sourceRows`, `deduplicatedRows`, `normalizedRows`.
- Sampling result: `partitioned.sampled` and `partitioned.skipped`.
- Timing metrics: `ingestElapsedMs`, `samplingElapsedMs`.
- Historical ingest/sampling logs via `logHistoricalIngestAndSampling(...)`.

## Steps

1. Read source rows with `readCatalogFile(...)`.
2. Deduplicate and normalize rows.
3. Fail if normalized set is empty.
4. Partition normalized rows into sampled/skipped sets.
5. Fail if sampled set is empty.
6. Emit sampling summary line and return stage payload.

## Failure Signals

- `No valid products found in the input file.`
- `Sampling selected 0 products (...)`
- File read/parse failures in ingest helpers.

## Related Files

- `src/pipeline/run-stage-ingest.ts`
- `src/pipeline/ingest.ts`
- `src/pipeline/run-support.ts`
- `src/pipeline/run.ts`

## Related Commands

- `npm run pipeline`
- `npm run canary`
- `npm run pipeline:env`

## Last Verified

- 2026-02-22

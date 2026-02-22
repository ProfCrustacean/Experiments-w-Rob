# Module Card: run-stage-startup

Owner: pipeline-flow-owner

## Purpose

Initialize run-level context before active enrichment work starts.

## When To Use

At the start of every pipeline run, after ingest and sampling are prepared and before categorization.

## Inputs

- Run identifiers: `runId`, `storeId`, `inputFileName`.
- Sampling metadata: `sampleParts`, `samplePartIndex`, sampled/skipped counts.
- Historical ingest/sampling timings and row counts.
- Runtime config (`AppConfig`) and run logger (`RunLogger`).

## Outputs

- Startup telemetry (`run.started` plus historical ingest/sampling stage events).
- A provider bundle from `createProviders(...)` wired with OpenAI telemetry callback.

## Steps

1. Validate `OPENAI_API_KEY` is present; fail fast if missing.
2. Emit `run.started` with run metadata and config snapshot.
3. Emit historical ingest and sampling stage logs with timings.
4. Create telemetry callback and return provider set.

## Failure Signals

- `OPENAI_API_KEY is missing` error.
- Startup logging fails due to malformed payload or logger issues.
- Provider creation throws before stage handoff.

## Related Files

- `src/pipeline/run-stage-startup.ts`
- `src/pipeline/run-stage-ingest.ts`
- `src/pipeline/run-support.ts`
- `src/pipeline/run.ts`

## Related Commands

- `npm run pipeline`
- `npm run pipeline:env`
- `npm run canary`

## Last Verified

- 2026-02-22

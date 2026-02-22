# Module Card: run-stage-artifact-generation

Owner: pipeline-flow-owner

## Purpose

Build final downloadable artifacts from run outputs and quality metrics.

## When To Use

After metrics are available and before artifact persistence/cleanup.

## Inputs

Run metadata, product/enrichment/category maps, QA and hotlist CSV content, quality counters.

## Outputs

Artifact payloads, artifact summaries, artifact expiration timestamps.

## Steps

1. Build report artifact payloads.\n2. Compute expiry timestamps.\n3. Log artifact generation stage metrics.

## Failure Signals

Artifact payload build failure, invalid artifact summary metadata, missing expected artifact keys.

## Related Files

- 
- 

## Related Commands

- 
> experiments-w-rob@0.1.0 pipeline
> tsx src/cli/pipeline.ts\n- 
> experiments-w-rob@0.1.0 report:list
> tsx src/cli/report-list.ts

## Last Verified

- 2026-02-22


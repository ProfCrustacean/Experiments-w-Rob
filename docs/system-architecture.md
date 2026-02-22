# System Architecture

High-level architecture for the current stage-based pipeline and self-improvement loop.

## Primary Flows

- Catalog pipeline (stage orchestrator): `src/pipeline/run.ts`
- Self-improvement orchestrator: `src/pipeline/self-improvement-orchestrator.ts`

## Architecture Maps

- Global system map: `docs/agents/maps/system-map.md`
- Pipeline stage call order: `docs/agents/maps/pipeline-stage-map.md`
- Self-improvement flow map: `docs/agents/maps/self-improvement-map.md`

## Pipeline Stage Modules

- `src/pipeline/run-stage-startup.ts`
- `src/pipeline/run-stage-ingest.ts`
- `src/pipeline/run-stage-categorize-enrich.ts`
- `src/pipeline/run-stage-embedding.ts`
- `src/pipeline/run-stage-product-persist.ts`
- `src/pipeline/run-stage-reporting.ts`
- `src/pipeline/run-stage-metrics.ts`
- `src/pipeline/run-stage-artifact-generation.ts`
- `src/pipeline/run-stage-artifact-persist.ts`
- `src/pipeline/run-stage-finalize.ts`

## Key Invariants

- Harness and schema checks gate learning apply paths.
- Canary loops use tiered apply behavior.
- Pipeline traces and artifacts are retained with cleanup stages.

## Where To Go Next

- Detailed module cards: `docs/agents/module-cards/pipeline`
- Operational safety standards: `docs/reliability-standards.md`


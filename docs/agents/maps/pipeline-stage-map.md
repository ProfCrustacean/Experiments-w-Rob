# Pipeline Stage Map

Owner: pipeline-flow-owner

Ordered call flow inside `src/pipeline/run.ts`.

1. Startup: `src/pipeline/run-stage-startup.ts`
2. Ingest + sampling: `src/pipeline/run-stage-ingest.ts`
3. Categorize + enrich: `src/pipeline/run-stage-categorize-enrich.ts`
4. Embedding: `src/pipeline/run-stage-embedding.ts`
5. Product persistence: `src/pipeline/run-stage-product-persist.ts`
6. Reporting: `src/pipeline/run-stage-reporting.ts`
7. Metrics: `src/pipeline/run-stage-metrics.ts`
8. Artifact generation: `src/pipeline/run-stage-artifact-generation.ts`
9. Artifact persistence + cleanup: `src/pipeline/run-stage-artifact-persist.ts`
10. Run finalize: `src/pipeline/run-stage-finalize.ts`

Module cards:

- `docs/agents/module-cards/pipeline/run-stage-startup.md`
- `docs/agents/module-cards/pipeline/run-stage-ingest.md`
- `docs/agents/module-cards/pipeline/run-stage-categorize-enrich.md`
- `docs/agents/module-cards/pipeline/run-stage-embedding.md`
- `docs/agents/module-cards/pipeline/run-stage-product-persist.md`
- `docs/agents/module-cards/pipeline/run-stage-reporting.md`
- `docs/agents/module-cards/pipeline/run-stage-metrics.md`
- `docs/agents/module-cards/pipeline/run-stage-artifact-generation.md`
- `docs/agents/module-cards/pipeline/run-stage-artifact-persist.md`
- `docs/agents/module-cards/pipeline/run-stage-finalize.md`
- `docs/agents/module-cards/pipeline/run-support.md`


# System Map

Owner: governance-policy-owner

## Runtime Surfaces

- Pipeline orchestrator: `src/pipeline/run.ts`
- Self-improvement orchestrator: `src/pipeline/self-improvement-orchestrator.ts`
- Docs checks: `src/docs/health-check.ts`
- Scoreboard builder: `src/docs/quality-scoreboard.ts`

## Data and Persistence

- Pipeline persistence: `src/pipeline/persist.ts`
- Self-improvement persistence: `src/pipeline/persist-self-improvement.ts`
- Migrations: `src/db/migrate.ts`

## External Entry Commands

- Pipeline: `src/cli/pipeline.ts`
- Canary: `src/cli/canary-run.ts`
- Worker: `src/cli/self-improve-worker.ts`
- Harness eval: `src/cli/harness-eval.ts`

## Related Docs

- `docs/agents/maps/pipeline-stage-map.md`
- `docs/agents/maps/self-improvement-map.md`
- `docs/system-architecture.md`


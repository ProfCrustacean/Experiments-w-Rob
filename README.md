# Product Enrichment Pipeline Prototype

Agent-first catalog enrichment pipeline for school-supply matching.

## Start Here

- Primary docs entrypoint: `docs/index.md`
- Agent navigation entrypoint: `docs/agents/index.md`

## What This Repository Does

- Ingests catalog files (`.csv` / `.xlsx`)
- Assigns product categories and attributes
- Produces QA and confusion artifacts
- Persists enriched products, vectors, and run telemetry
- Runs a gated self-improvement loop (`canary` and `full`)

## Quick Setup

```bash
npm install
cp .env.example .env
npm run db:migrate
```

Minimum environment values:

- `DATABASE_URL`
- `OPENAI_API_KEY`
- `STORE_ID`

## Most Used Commands

Pipeline and canary:

```bash
npm run pipeline -- --input ./data/catalog.xlsx --store continente --run-label pilot-1
npm run canary
```

Self-improvement:

```bash
npm run self-improve:worker
npm run self-improve:enqueue -- --phrase "run 3 self-improvement canary loops"
npm run self-improve:status -- --phrase "show self-improvement batches"
```

Harness and learning:

```bash
npm run harness:build -- --store continente
npm run harness:eval -- --store continente
npm run learn:propose -- --batch-id <batchId> --run-id <runId>
npm run learn:apply -- --batch-id <batchId> --run-id <runId>
```

Docs checks:

```bash
npm run docs:check
npm run docs:scoreboard
```

## Documentation Map

Top-level entry docs:

- `docs/index.md`
- `docs/system-architecture.md`
- `docs/self-improvement-runbook.md`
- `docs/reliability-standards.md`
- `docs/decisions-log.md`
- `docs/quality-scoreboard.md`

Agent-first docs:

- `docs/agents/index.md`
- `docs/agents/maps/system-map.md`
- `docs/agents/maps/pipeline-stage-map.md`
- `docs/agents/maps/self-improvement-map.md`
- `docs/agents/maps/command-map.md`
- `docs/agents/maps/ownership-map.md`

Governance docs:

- `docs/governance/docs-policy.md`
- `docs/governance/docs-check-spec.md`

## CI and Enforcement

- Docs gate (blocking): `.github/workflows/docs-gate.yml`
- Harness gate (blocking): `.github/workflows/harness-gate.yml`
- Docs garden (daily refresh): `.github/workflows/docs-garden.yml`


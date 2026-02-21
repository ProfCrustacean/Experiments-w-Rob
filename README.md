# Product Enrichment Pipeline Prototype

Prototype pipeline to enrich catalog products for school-supply matching.

## Status

[![Harness Gate](https://github.com/ProfCrustacean/Experiments-w-Rob/actions/workflows/harness-gate.yml/badge.svg?branch=main)](https://github.com/ProfCrustacean/Experiments-w-Rob/actions/workflows/harness-gate.yml)
[![Docs Gate](https://github.com/ProfCrustacean/Experiments-w-Rob/actions/workflows/docs-gate.yml/badge.svg?branch=main)](https://github.com/ProfCrustacean/Experiments-w-Rob/actions/workflows/docs-gate.yml)

## What it does

- Reads a catalog file (`.csv` or `.xlsx`)
- Groups products into subclass categories
- Generates Portuguese category names, descriptions, and JSON attribute schemas
- Extracts category-specific attributes per product
- Writes categories and products to PostgreSQL (`JSONB`)
- Creates product embeddings and stores vectors with `pgvector`
- Produces full downloadable run reports (XLSX + CSV) and QA CSV

## Documentation map

- Main setup and command reference: this file
- Self-improvement operations runbook: `docs/self-improvement-runbook.md`
- Architecture overview: `docs/system-architecture.md`
- Reliability standards: `docs/reliability-standards.md`
- Decisions log: `docs/decisions-log.md`
- Quality scoreboard: `docs/quality-scoreboard.md`

## Requirements

- Node.js 20+
- PostgreSQL with extension support for `pgvector`
- Environment variables in `.env`

## Setup

```bash
npm install
cp .env.example .env
```

Set at minimum:

- `DATABASE_URL`
- `OPENAI_API_KEY` (recommended for production quality; optional fallback mode exists for local/testing use)
- `CATALOG_INPUT_PATH` (for `npm run canary`)
- `STORE_ID` (for `npm run canary`)

Useful throughput controls:

- `INPUT_SAMPLE_PARTS`, `INPUT_SAMPLE_PART_INDEX`
- `CATEGORY_PROFILE_CONCURRENCY`
- `ATTRIBUTE_BATCH_SIZE`, `ATTRIBUTE_LLM_CONCURRENCY`
- `EMBEDDING_BATCH_SIZE`, `EMBEDDING_CONCURRENCY`
- `OPENAI_TIMEOUT_MS`, `OPENAI_MAX_RETRIES`, `OPENAI_RETRY_BASE_MS`, `OPENAI_RETRY_MAX_MS`

Canary controls:

- `CANARY_SAMPLE_SIZE` (default `350`)
- `CANARY_FIXED_RATIO` (default `0.30`, so 105 fixed + 245 random at sample size 350)
- `CANARY_RANDOM_SEED` (default `canary-v1`, keeps random subset deterministic)
- `CANARY_AUTO_ACCEPT_THRESHOLD` (default `0.80`)
- `CANARY_SUBSET_PATH` (default `outputs/canary_input.csv`)
- `CANARY_STATE_PATH` (default `outputs/canary_state.json`)

Self-improvement controls:

- `SELF_IMPROVE_MAX_LOOPS` (default `10`, hard cap per request)
- `SELF_IMPROVE_RETRY_LIMIT` (default `1`)
- `SELF_IMPROVE_AUTO_APPLY_POLICY` (default `if_gate_passes`)
- `SELF_IMPROVE_WORKER_POLL_MS` (default `5000`)
- `SELF_IMPROVE_STALE_RUN_TIMEOUT_MINUTES` (default `30`, auto-recovers stale `running` attempts/batches)
- `SELF_IMPROVE_MAX_STRUCTURAL_CHANGES_PER_LOOP` (default `2`)
- `SELF_IMPROVE_GATE_MIN_SAMPLE_SIZE` (default `200`)
- `SELF_IMPROVE_POST_APPLY_WATCH_LOOPS` (default `2`)
- `SELF_IMPROVE_ROLLBACK_ON_DEGRADE` (default `true`)
- `SELF_IMPROVE_CANARY_RETRY_DEGRADE_ENABLED` (default `true`)
- `SELF_IMPROVE_CANARY_RETRY_MIN_PROPOSAL_CONFIDENCE` (default `0.75`)

Harness thresholds:

- `HARNESS_MIN_L1_DELTA` (default `0`)
- `HARNESS_MIN_L2_DELTA` (default `0`)
- `HARNESS_MIN_L3_DELTA` (default `0`)
- `HARNESS_MAX_FALLBACK_RATE` (default `0.06`)
- `HARNESS_MAX_NEEDS_REVIEW_RATE` (default `0.35`)

Report retention:

- `ARTIFACT_RETENTION_HOURS` (default `24`)
- `TRACE_RETENTION_HOURS` (default `24`)
- `TRACE_FLUSH_BATCH_SIZE` (default `25`)
- `STALE_RUN_TIMEOUT_MINUTES` (default `180`, auto-marks abandoned `running` `pipeline_runs` as `failed`)
- `PRODUCT_PERSIST_STAGE_TIMEOUT_MS` (default `60000`, fail-fast guard for product persistence stage)
- `PRODUCT_VECTOR_QUERY_TIMEOUT_MS` (default `20000`, per-query DB timeout for vector writes)
- `PRODUCT_VECTOR_BATCH_SIZE` (default `100`)

## Commands

Run migrations:

```bash
npm run db:migrate
```

Run enrichment pipeline:

```bash
npm run pipeline -- --input ./data/catalog.xlsx --store continente --run-label pilot-1
```

Run a deterministic half-sample (part `0` of `2`):

```bash
npm run pipeline -- --input ./data/catalog.xlsx --store continente --run-label pilot-1 --sample-parts 2 --sample-part-index 0
```

Run enrichment pipeline from environment variables (useful for Render cron jobs):

```bash
CATALOG_INPUT_PATH=./data/catalog.xlsx STORE_ID=continente RUN_LABEL=render-pilot npm run pipeline:env
```

Run canary pipeline (350-product default subset + quality gate):

```bash
npm run canary
```

Enqueue a queued self-improvement batch:

```bash
npm run self-improve:enqueue -- --type canary --count 5
```

Enqueue from a natural-language phrase:

```bash
npm run self-improve:enqueue -- --phrase "run 3 self-improvement full loops"
```

Run one worker cycle (process one queued batch, else idle exit):

```bash
npm run self-improve:worker -- --once
```

Each worker cycle also performs stale recovery for:
- self-improvement attempts/batches (`SELF_IMPROVE_STALE_RUN_TIMEOUT_MINUTES`)
- pipeline runs for the configured store (`STALE_RUN_TIMEOUT_MINUTES`)

Run worker continuously:

```bash
npm run self-improve:worker
```

Show self-improvement batch status from natural-language phrase:

```bash
npm run self-improve:status -- --phrase "show self-improvement batches"
npm run self-improve:status -- --phrase "show self-improvement batch <batchId>"
```

Cancel a queued/running self-improvement batch:

```bash
npm run self-improve:cancel -- --batch-id <batchId>
```

Build a benchmark snapshot for harness evaluation:

```bash
npm run harness:build -- --store continente
```

Evaluate a run against harness thresholds:

```bash
npm run harness:eval -- --store continente --candidate-run-id <runId>
```

`harness:eval` now auto-refreshes stale/undersized benchmark snapshots and auto-top-ups sample coverage to the configured minimum, so manual benchmark babysitting is not required.

Generate learning proposals for a run/batch:

```bash
npm run learn:propose -- --batch-id <batchId> --run-id <runId>
```

Apply pending proposals with harness gating:

```bash
npm run learn:apply -- --batch-id <batchId> --run-id <runId>
```

Rollback the latest applied change:

```bash
npm run learn:rollback -- --latest
```

Run docs freshness checks:

```bash
npm run docs:check
```

Refresh quality scoreboard from live metrics:

```bash
npm run docs:scoreboard
```

Codex trigger phrases (natural language contract):

- `run <N> self-improvement canary loops`
- `run <N> self-improvement full loops`
- `run <N> self-improvement canary loop`
- `run <N> self-improvement full loop`
- `show self-improvement batches`
- `show self-improvement batch <batchId>`

Validation rules:

- `N` must be an integer from `1` to `10`
- Requests above `10` are rejected with a clear message
- Ambiguous phrases are rejected with a clear correction hint

Self-improvement policy (current defaults):

- Execution mode: queued async worker
- Per-batch retry: one retry for runtime/system failures only
- Auto-apply policy: apply only when gates pass
- Structural learning apply cap: max `2` structural changes per loop
- Canary retry degrade mode: on canary retries, disable structural proposals and require higher proposal confidence
- Post-apply safety: monitor next `2` loops and rollback on degrade when enabled

Canary behavior:

- Selects fixed+random products from your catalog (`30%` fixed from hotlist, `70%` deterministic random)
- Runs full pipeline only for that subset
- Uses `auto_accepted_rate >= 0.80` as the default pass/fail gate
- Updates `outputs/canary_state.json` to point to the newly generated canary hotlist
- Next canary prefers that last canary hotlist (rolling loop)

Enable automatic canary before each push:

```bash
git config core.hooksPath .githooks
```

Bypass once (emergency only):

```bash
SKIP_CANARY=1 git push
```

Output:

- Terminal JSON summary (includes `runId`)
- QA report CSV in `outputs/qa_report_<runId>.csv`
- Full run CSV in `outputs/pipeline_output_<runId>.csv`
- Full run XLSX in `outputs/pipeline_output_<runId>.xlsx`
- All three files are also stored in PostgreSQL for download from Render runs
- Structured run traces are streamed live to Render logs and persisted in PostgreSQL

List recent runs and available download formats:

```bash
npm run report:list -- --store continente --limit 20
```

Download one artifact by run ID:

```bash
npm run report:download -- --run-id <runId> --format xlsx --out ./outputs
```

Supported `--format` values: `xlsx`, `csv`, `qa-csv`, `confusion-csv`.

Artifacts expire after `ARTIFACT_RETENTION_HOURS` (24h by default). Download commands return a clear expiry error when files are no longer available.

Query detailed logs for a run:

```bash
npm run logs:run -- --run-id <runId> --limit 500
```

Optional filters for `logs:run`: `--stage`, `--event`, `--level`, `--include-expired`.

Download all logs for a run as JSONL:

```bash
npm run logs:download -- --run-id <runId> --out ./outputs
```

Run logs expire after `TRACE_RETENTION_HOURS` (24h by default).

Evaluate QA report and update run status (`accepted`/`rejected`):

```bash
npm run qa:evaluate -- --file ./outputs/qa_report_<runId>.csv --threshold 0.85
```

Use `--no-db` to evaluate only (no DB status update).

## Input columns

Required:

- `source_sku` (or aliases like `sku`, `codigo`)
- `title` (or aliases like `nome`, `name`)

Optional:

- `description`, `brand`, `price`, `availability`, `url`, `image_url`

## Database tables

- `pipeline_runs`
- `pipeline_run_logs`
- `pipeline_run_artifacts`
- `pipeline_qa_feedback`
- `categories`
- `category_synonyms`
- `products`
- `product_vectors`
- `self_improvement_batches`
- `self_improvement_batch_runs`
- `self_improvement_proposals`
- `self_improvement_proposal_diffs`
- `self_improvement_applied_changes`
- `self_improvement_rollback_events`
- `self_improvement_harness_runs`
- `self_improvement_benchmark_snapshots`

## CI harness gate

GitHub Actions workflow: `.github/workflows/harness-gate.yml`

Required repository settings:

- Secret: `DATABASE_URL`
- Variable: `STORE_ID`
- Optional variable: `HARNESS_CANDIDATE_RUN_ID` (if omitted, the latest eligible run is used)

The workflow runs:

- `npm ci`
- `npm run lint`
- `npm test`
- `npm run docs:check`
- `npm run harness:build`
- `npm run harness:eval`

## Docs governance

- PR/main docs gate: `.github/workflows/docs-gate.yml`
- Daily docs gardening run: `.github/workflows/docs-garden.yml`
- Local docs check command: `npm run docs:check`
- Local scoreboard refresh command: `npm run docs:scoreboard`
- Scoreboard freshness SLA: max 48 hours old
- Optional repo variable for issue assignment: `DOCS_OWNER`

## Code map (self-improvement)

- Batch orchestration: `src/pipeline/self-improvement-orchestrator.ts`
- Per-loop execution and gated learning: `src/pipeline/self-improvement-loop.ts`
- Phrase parsing contract: `src/pipeline/self-improvement-phrase.ts`
- Self-improvement persistence: `src/pipeline/persist-self-improvement.ts`
- Rule patch helpers: `src/pipeline/rule-patch.ts`
- Operational runbook: `docs/self-improvement-runbook.md`

## Testing

```bash
npm test
```

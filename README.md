# Product Enrichment Pipeline Prototype

Prototype pipeline to enrich catalog products for school-supply matching.

## What it does

- Reads a catalog file (`.csv` or `.xlsx`)
- Groups products into subclass categories
- Generates Portuguese category names, descriptions, and JSON attribute schemas
- Extracts category-specific attributes per product
- Writes categories and products to PostgreSQL (`JSONB`)
- Creates product embeddings and stores vectors with `pgvector`
- Produces full downloadable run reports (XLSX + CSV) and QA CSV

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
- `OPENAI_API_KEY` (optional but recommended; fallback mode works without it)

Useful throughput controls:

- `INPUT_SAMPLE_PARTS`, `INPUT_SAMPLE_PART_INDEX`
- `CATEGORY_PROFILE_CONCURRENCY`
- `ATTRIBUTE_BATCH_SIZE`, `ATTRIBUTE_LLM_CONCURRENCY`
- `EMBEDDING_BATCH_SIZE`, `EMBEDDING_CONCURRENCY`
- `OPENAI_TIMEOUT_MS`, `OPENAI_MAX_RETRIES`, `OPENAI_RETRY_BASE_MS`, `OPENAI_RETRY_MAX_MS`

Report retention:

- `ARTIFACT_RETENTION_HOURS` (default `24`)

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

Output:

- Terminal JSON summary (includes `runId`)
- QA report CSV in `outputs/qa_report_<runId>.csv`
- Full run CSV in `outputs/pipeline_output_<runId>.csv`
- Full run XLSX in `outputs/pipeline_output_<runId>.xlsx`
- All three files are also stored in PostgreSQL for download from Render runs

List recent runs and available download formats:

```bash
npm run report:list -- --store continente --limit 20
```

Download one artifact by run ID:

```bash
npm run report:download -- --run-id <runId> --format xlsx --out ./outputs
```

Supported `--format` values: `xlsx`, `csv`, `qa-csv`.

Artifacts expire after `ARTIFACT_RETENTION_HOURS` (24h by default). Download commands return a clear expiry error when files are no longer available.

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
- `pipeline_run_artifacts`
- `categories`
- `category_synonyms`
- `products`
- `product_vectors`

## Testing

```bash
npm test
```

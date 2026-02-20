# Product Enrichment Pipeline Prototype

Prototype pipeline to enrich catalog products for school-supply matching.

## What it does

- Reads a catalog file (`.csv` or `.xlsx`)
- Groups products into subclass categories
- Generates Portuguese category names, descriptions, and JSON attribute schemas
- Extracts category-specific attributes per product
- Writes categories and products to PostgreSQL (`JSONB`)
- Creates product embeddings and stores vectors with `pgvector`
- Produces a QA CSV sample for manual validation

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

## Commands

Run migrations:

```bash
npm run db:migrate
```

Run enrichment pipeline:

```bash
npm run pipeline -- --input ./data/catalog.xlsx --store continente --run-label pilot-1
```

Run enrichment pipeline from environment variables (useful for Render cron jobs):

```bash
CATALOG_INPUT_PATH=./data/catalog.xlsx STORE_ID=continente RUN_LABEL=render-pilot npm run pipeline:env
```

Output:

- Terminal JSON summary (includes `runId`)
- QA report CSV in `outputs/qa_report_<runId>.csv`

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
- `categories`
- `category_synonyms`
- `products`
- `product_vectors`

## Testing

```bash
npm test
```

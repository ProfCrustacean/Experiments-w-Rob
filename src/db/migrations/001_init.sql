CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS pipeline_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  store_id text NOT NULL,
  input_file_name text NOT NULL,
  run_label text,
  status text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT NOW(),
  finished_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT NOW(),
  stats_json jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_store_id ON pipeline_runs(store_id);

CREATE TABLE IF NOT EXISTS categories (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  store_id text NOT NULL,
  name_pt text NOT NULL,
  slug text NOT NULL,
  description_pt text NOT NULL,
  attributes_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT NOW(),
  updated_at timestamptz NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_categories_store_slug UNIQUE (store_id, slug)
);

CREATE INDEX IF NOT EXISTS idx_categories_attributes_gin ON categories USING gin (attributes_jsonb);

DROP TRIGGER IF EXISTS trg_categories_updated_at ON categories;
CREATE TRIGGER trg_categories_updated_at
BEFORE UPDATE ON categories
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS category_synonyms (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  category_id uuid NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
  term_pt text NOT NULL,
  source text NOT NULL CHECK (source IN ('ai', 'manual')),
  created_at timestamptz NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_category_synonyms UNIQUE (category_id, term_pt)
);

CREATE INDEX IF NOT EXISTS idx_category_synonyms_term ON category_synonyms(term_pt);

CREATE TABLE IF NOT EXISTS products (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  store_id text NOT NULL,
  source_sku text NOT NULL,
  title text NOT NULL,
  description text,
  brand text,
  price numeric(12, 2),
  availability text,
  url text,
  image_url text,
  category_id uuid REFERENCES categories(id),
  category_confidence real NOT NULL DEFAULT 0,
  attribute_values_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  attribute_confidence_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
  needs_review boolean NOT NULL DEFAULT false,
  run_id uuid REFERENCES pipeline_runs(id),
  created_at timestamptz NOT NULL DEFAULT NOW(),
  updated_at timestamptz NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_products_store_sku UNIQUE (store_id, source_sku)
);

CREATE INDEX IF NOT EXISTS idx_products_category ON products(category_id);
CREATE INDEX IF NOT EXISTS idx_products_run_id ON products(run_id);
CREATE INDEX IF NOT EXISTS idx_products_attributes_gin ON products USING gin (attribute_values_jsonb);

DROP TRIGGER IF EXISTS trg_products_updated_at ON products;
CREATE TRIGGER trg_products_updated_at
BEFORE UPDATE ON products
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS product_vectors (
  product_id uuid PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
  embedding vector(3072) NOT NULL,
  embedding_model text NOT NULL,
  embedded_text text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
  BEGIN
    EXECUTE 'CREATE INDEX idx_product_vectors_embedding_hnsw ON product_vectors USING hnsw (embedding vector_cosine_ops)';
  EXCEPTION
    WHEN duplicate_table THEN
      NULL;
    WHEN duplicate_object THEN
      NULL;
    WHEN OTHERS THEN
      BEGIN
        EXECUTE 'CREATE INDEX idx_product_vectors_embedding_ivfflat ON product_vectors USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)';
      EXCEPTION
        WHEN duplicate_table THEN
          NULL;
        WHEN duplicate_object THEN
          NULL;
      END;
  END;
END;
$$;

DROP TRIGGER IF EXISTS trg_pipeline_runs_updated_at ON pipeline_runs;
CREATE TRIGGER trg_pipeline_runs_updated_at
BEFORE UPDATE ON pipeline_runs
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

import type { PoolClient } from "pg";
import { getPool } from "../db/client.js";
import type {
  CategoryDraft,
  NormalizedCatalogProduct,
  PersistedCategory,
  PersistedProduct,
  ProductEnrichment,
} from "../types.js";
import { vectorToSqlLiteral } from "./embedding.js";

export async function createPipelineRun(input: {
  storeId: string;
  inputFileName: string;
  runLabel?: string;
}): Promise<string> {
  const pool = getPool();
  const result = await pool.query<{ id: string }>(
    `
      INSERT INTO pipeline_runs (store_id, input_file_name, run_label, status)
      VALUES ($1, $2, $3, 'running')
      RETURNING id
    `,
    [input.storeId, input.inputFileName, input.runLabel ?? null],
  );

  return result.rows[0].id;
}

export async function finalizePipelineRun(input: {
  runId: string;
  status: "completed_pending_review" | "accepted" | "rejected" | "failed";
  stats: Record<string, unknown>;
}): Promise<void> {
  const pool = getPool();
  await pool.query(
    `
      UPDATE pipeline_runs
      SET status = $2,
          finished_at = NOW(),
          stats_json = $3::jsonb
      WHERE id = $1
    `,
    [input.runId, input.status, JSON.stringify(input.stats)],
  );
}

async function upsertCategory(
  client: PoolClient,
  storeId: string,
  draft: CategoryDraft,
): Promise<PersistedCategory> {
  const result = await client.query<PersistedCategory>(
    `
      INSERT INTO categories (store_id, name_pt, slug, description_pt, attributes_jsonb)
      VALUES ($1, $2, $3, $4, $5::jsonb)
      ON CONFLICT (store_id, slug)
      DO UPDATE SET
        name_pt = EXCLUDED.name_pt,
        description_pt = EXCLUDED.description_pt,
        attributes_jsonb = EXCLUDED.attributes_jsonb,
        updated_at = NOW()
      RETURNING id, slug, name_pt, description_pt, attributes_jsonb
    `,
    [storeId, draft.name_pt, draft.slug, draft.description_pt, JSON.stringify(draft.attributes_jsonb)],
  );

  const category = result.rows[0];

  await client.query(
    `
      DELETE FROM category_synonyms
      WHERE category_id = $1
        AND source = 'ai'
    `,
    [category.id],
  );

  for (const synonym of draft.synonyms) {
    await client.query(
      `
        INSERT INTO category_synonyms (category_id, term_pt, source)
        VALUES ($1, $2, 'ai')
        ON CONFLICT (category_id, term_pt) DO NOTHING
      `,
      [category.id, synonym],
    );
  }

  return {
    ...category,
    attributes_jsonb: draft.attributes_jsonb,
  };
}

export async function upsertCategoryDrafts(
  storeId: string,
  drafts: CategoryDraft[],
): Promise<Map<string, PersistedCategory>> {
  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const bySlug = new Map<string, PersistedCategory>();
    for (const draft of drafts) {
      const category = await upsertCategory(client, storeId, draft);
      bySlug.set(draft.slug, category);
    }

    await client.query("COMMIT");
    return bySlug;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function upsertProducts(input: {
  storeId: string;
  runId: string;
  products: NormalizedCatalogProduct[];
  enrichments: Map<string, ProductEnrichment>;
  categoriesBySlug: Map<string, PersistedCategory>;
}): Promise<Map<string, PersistedProduct>> {
  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const bySku = new Map<string, PersistedProduct>();

    for (const product of input.products) {
      const enrichment = input.enrichments.get(product.sourceSku);
      const category = enrichment
        ? input.categoriesBySlug.get(enrichment.categorySlug) ?? null
        : null;

      const result = await client.query<PersistedProduct>(
        `
          INSERT INTO products (
            store_id,
            source_sku,
            title,
            description,
            brand,
            price,
            availability,
            url,
            image_url,
            category_id,
            category_confidence,
            attribute_values_jsonb,
            attribute_confidence_jsonb,
            needs_review,
            run_id
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14,$15
          )
          ON CONFLICT (store_id, source_sku)
          DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            brand = EXCLUDED.brand,
            price = EXCLUDED.price,
            availability = EXCLUDED.availability,
            url = EXCLUDED.url,
            image_url = EXCLUDED.image_url,
            category_id = EXCLUDED.category_id,
            category_confidence = EXCLUDED.category_confidence,
            attribute_values_jsonb = EXCLUDED.attribute_values_jsonb,
            attribute_confidence_jsonb = EXCLUDED.attribute_confidence_jsonb,
            needs_review = EXCLUDED.needs_review,
            run_id = EXCLUDED.run_id,
            updated_at = NOW()
          RETURNING id, source_sku, title, category_id, category_confidence, needs_review, attribute_values_jsonb
        `,
        [
          input.storeId,
          product.sourceSku,
          product.title,
          product.description ?? null,
          product.brand ?? null,
          product.price ?? null,
          product.availability ?? null,
          product.url ?? null,
          product.imageUrl ?? null,
          category?.id ?? null,
          enrichment?.categoryConfidence ?? 0,
          JSON.stringify(enrichment?.attributeValues ?? {}),
          JSON.stringify(enrichment?.attributeConfidence ?? {}),
          enrichment?.needsReview ?? true,
          input.runId,
        ],
      );

      bySku.set(product.sourceSku, result.rows[0]);
    }

    await client.query("COMMIT");
    return bySku;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function upsertProductVectors(input: {
  productBySku: Map<string, PersistedProduct>;
  vectorsBySku: Map<string, number[]>;
  embeddedTextBySku: Map<string, string>;
  embeddingModel: string;
}): Promise<void> {
  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (const [sku, persistedProduct] of input.productBySku.entries()) {
      const vector = input.vectorsBySku.get(sku);
      const text = input.embeddedTextBySku.get(sku);
      if (!vector || !text) {
        continue;
      }

      await client.query(
        `
          INSERT INTO product_vectors (product_id, embedding, embedding_model, embedded_text)
          VALUES ($1, $2::vector, $3, $4)
          ON CONFLICT (product_id)
          DO UPDATE SET
            embedding = EXCLUDED.embedding,
            embedding_model = EXCLUDED.embedding_model,
            embedded_text = EXCLUDED.embedded_text,
            updated_at = NOW()
        `,
        [persistedProduct.id, vectorToSqlLiteral(vector), input.embeddingModel, text],
      );
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function updateRunStatusFromQAEvaluation(input: {
  runId: string;
  passRate: number;
  threshold: number;
  stats: Record<string, unknown>;
}): Promise<void> {
  const status = input.passRate >= input.threshold ? "accepted" : "rejected";
  await finalizePipelineRun({
    runId: input.runId,
    status,
    stats: {
      ...input.stats,
      qa_pass_rate: input.passRate,
      qa_threshold: input.threshold,
    },
  });
}

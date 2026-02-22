import { RunLogger } from "../logging/run-logger.js";
import type {
  NormalizedCatalogProduct,
  PersistedCategory,
  ProductEnrichment,
} from "../types.js";
import { upsertProducts, upsertProductVectors } from "./persist.js";

export interface ProductPersistenceStageResult {
  persistedProductCount: number;
  vectorPersistedRows: number;
  vectorPersistedBatches: number;
}

export async function runProductPersistenceStage(input: {
  storeId: string;
  runId: string;
  products: NormalizedCatalogProduct[];
  enrichments: Map<string, ProductEnrichment>;
  categoriesBySlug: Map<string, PersistedCategory>;
  vectorsBySku: Map<string, number[]>;
  embeddedTextBySku: Map<string, string>;
  embeddingModel: string;
  queryTimeoutMs: number;
  vectorBatchSize: number;
  persistStageTimeoutMs: number;
  logger: RunLogger;
  stageTimingsMs: Record<string, number>;
}): Promise<ProductPersistenceStageResult> {
  input.logger.info("pipeline", "stage.started", "Starting product persistence stage.", {
    stage_name: "product_persist",
  });

  const productPersistStart = Date.now();
  const productUpsertBatchSize = 50;

  input.logger.info("pipeline", "substage.started", "Starting products upsert substage.", {
    stage_name: "product_persist",
    substage_name: "products_upsert",
    timeout_ms: input.persistStageTimeoutMs,
    query_timeout_ms: input.queryTimeoutMs,
    batch_size: productUpsertBatchSize,
    product_count: input.products.length,
  });

  const productUpsertStart = Date.now();
  const persistedProducts = await upsertProducts({
    storeId: input.storeId,
    runId: input.runId,
    products: input.products,
    enrichments: input.enrichments,
    categoriesBySlug: input.categoriesBySlug,
    queryTimeoutMs: input.queryTimeoutMs,
    batchSize: productUpsertBatchSize,
    stageTimeoutMs: input.persistStageTimeoutMs,
    timeoutStageName: "product_persist.products_upsert",
    onProgress: (progress) => {
      input.logger.debug(
        "pipeline",
        "substage.products_upsert.progress",
        "Products upsert progress updated.",
        {
          stage_name: "product_persist",
          substage_name: "products_upsert",
          processed: progress.processed,
          total: progress.total,
        },
      );
    },
  });

  const persistedProductCount = persistedProducts.size;
  input.stageTimingsMs.product_upsert_ms = Date.now() - productUpsertStart;
  input.logger.info("pipeline", "substage.completed", "Products upsert substage completed.", {
    stage_name: "product_persist",
    substage_name: "products_upsert",
    elapsed_ms: input.stageTimingsMs.product_upsert_ms,
    persisted_products: persistedProductCount,
  });

  input.logger.info("pipeline", "substage.started", "Starting vectors upsert substage.", {
    stage_name: "product_persist",
    substage_name: "vectors_upsert",
    timeout_ms: input.persistStageTimeoutMs,
    query_timeout_ms: input.queryTimeoutMs,
    batch_size: input.vectorBatchSize,
  });

  const vectorUpsertStart = Date.now();
  const vectorUpsertResult = await upsertProductVectors({
    productBySku: persistedProducts,
    vectorsBySku: input.vectorsBySku,
    embeddedTextBySku: input.embeddedTextBySku,
    embeddingModel: input.embeddingModel,
    queryTimeoutMs: input.queryTimeoutMs,
    batchSize: input.vectorBatchSize,
    stageTimeoutMs: input.persistStageTimeoutMs,
    timeoutStageName: "product_persist.vectors_upsert",
    onBatchProgress: (progress) => {
      input.logger.debug(
        "pipeline",
        "substage.vectors_upsert.progress",
        "Product vectors upsert progress updated.",
        {
          stage_name: "product_persist",
          substage_name: "vectors_upsert",
          processed_batches: progress.processedBatches,
          total_batches: progress.totalBatches,
          processed_rows: progress.processedRows,
          total_rows: progress.totalRows,
        },
      );
    },
  });

  const vectorPersistedRows = vectorUpsertResult.totalRows;
  const vectorPersistedBatches = vectorUpsertResult.totalBatches;
  input.stageTimingsMs.product_vector_upsert_ms = Date.now() - vectorUpsertStart;
  input.logger.info("pipeline", "substage.completed", "Vectors upsert substage completed.", {
    stage_name: "product_persist",
    substage_name: "vectors_upsert",
    elapsed_ms: input.stageTimingsMs.product_vector_upsert_ms,
    persisted_vectors: vectorPersistedRows,
    persisted_vector_batches: vectorPersistedBatches,
  });

  input.stageTimingsMs.product_persist_ms = Date.now() - productPersistStart;
  input.logger.info("pipeline", "stage.completed", "Product persistence stage completed.", {
    stage_name: "product_persist",
    elapsed_ms: input.stageTimingsMs.product_persist_ms,
    persisted_products: persistedProductCount,
    persisted_vectors: vectorPersistedRows,
    persisted_vector_batches: vectorPersistedBatches,
    substage_timeout_ms: input.persistStageTimeoutMs,
    vector_query_timeout_ms: input.queryTimeoutMs,
  });

  return {
    persistedProductCount,
    vectorPersistedRows,
    vectorPersistedBatches,
  };
}

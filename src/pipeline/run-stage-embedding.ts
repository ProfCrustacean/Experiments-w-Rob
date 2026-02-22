import { RunLogger } from "../logging/run-logger.js";
import type {
  EmbeddingProvider,
  NormalizedCatalogProduct,
  PersistedCategory,
  ProductEnrichment,
} from "../types.js";
import { buildEmbeddingText, generateEmbeddingsForItems } from "./embedding.js";
import { EMBEDDING_DIMENSIONS, ensureVectorLength, shouldLogProgress } from "./run-support.js";

export interface EmbeddingStageResult {
  vectorsBySku: Map<string, number[]>;
  embeddedTextBySku: Map<string, string>;
}

export async function runEmbeddingStage(input: {
  products: NormalizedCatalogProduct[];
  enrichments: Map<string, ProductEnrichment>;
  categoriesBySlug: Map<string, PersistedCategory>;
  embeddingProvider: EmbeddingProvider;
  embeddingBatchSize: number;
  embeddingConcurrency: number;
  logger: RunLogger;
  stageTimingsMs: Record<string, number>;
}): Promise<EmbeddingStageResult> {
  input.logger.info("pipeline", "stage.started", "Starting embedding stage.", {
    stage_name: "embedding",
  });

  const embeddingStart = Date.now();
  const embeddingWorkItems: Array<{ sourceSku: string; text: string }> = [];
  const embeddedTextBySku = new Map<string, string>();

  for (const product of input.products) {
    const enrichment = input.enrichments.get(product.sourceSku);
    if (!enrichment) {
      continue;
    }

    const category = input.categoriesBySlug.get(enrichment.categorySlug);
    const categoryName = category?.name_pt ?? "material escolar";
    const text = buildEmbeddingText({
      product,
      categoryName,
      attributeValues: enrichment.attributeValues,
    });

    embeddingWorkItems.push({ sourceSku: product.sourceSku, text });
    embeddedTextBySku.set(product.sourceSku, text);
  }

  const rawVectors = await generateEmbeddingsForItems(
    embeddingWorkItems,
    input.embeddingProvider,
    input.embeddingBatchSize,
    input.embeddingConcurrency,
    (done, total) => {
      if (shouldLogProgress(done, total)) {
        // eslint-disable-next-line no-console
        console.log(`[embeddings] ${done}/${total} batches completed`);
        input.logger.info(
          "pipeline",
          "batch.embedding.completed",
          "Embedding batch progress updated.",
          {
            completed: done,
            total,
          },
        );
      }
    },
  );

  const vectorsBySku = new Map<string, number[]>();
  for (const [sku, vector] of rawVectors.entries()) {
    vectorsBySku.set(sku, ensureVectorLength(vector, EMBEDDING_DIMENSIONS));
  }

  input.stageTimingsMs.embedding_ms = Date.now() - embeddingStart;
  input.logger.info("pipeline", "stage.completed", "Embedding stage completed.", {
    stage_name: "embedding",
    elapsed_ms: input.stageTimingsMs.embedding_ms,
    embedded_products: vectorsBySku.size,
  });

  return {
    vectorsBySku,
    embeddedTextBySku,
  };
}

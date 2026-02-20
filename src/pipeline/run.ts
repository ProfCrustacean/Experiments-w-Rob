import path from "node:path";
import pLimit from "p-limit";
import { getConfig } from "../config.js";
import type {
  EmbeddingProvider,
  LLMProvider,
  NormalizedCatalogProduct,
  PipelineRunSummary,
  ProductEnrichment,
} from "../types.js";
import { runMigrations } from "../db/migrate.js";
import { clusterProducts } from "./categorization.js";
import { buildEmbeddingText, generateEmbeddingsForItems } from "./embedding.js";
import { enrichProduct } from "./enrichment.js";
import { generateCategoryDrafts } from "./category-generation.js";
import { deduplicateRows, normalizeRows, readCatalogFile } from "./ingest.js";
import {
  createPipelineRun,
  finalizePipelineRun,
  upsertCategoryDrafts,
  upsertProducts,
  upsertProductVectors,
} from "./persist.js";
import { writeQAReport } from "./qa-report.js";
import { FallbackProvider } from "../services/fallback.js";
import { OpenAIProvider } from "../services/openai.js";

const EMBEDDING_DIMENSIONS = 1536;

interface RunPipelineInput {
  inputPath: string;
  storeId: string;
  runLabel?: string;
}

function ensureVectorLength(vector: number[], target: number): number[] {
  if (vector.length === target) {
    return vector;
  }

  if (vector.length > target) {
    return vector.slice(0, target);
  }

  return [...vector, ...new Array(target - vector.length).fill(0)];
}

function createProviders(): {
  embeddingProvider: EmbeddingProvider;
  llmProvider: LLMProvider;
  usingOpenAI: boolean;
} {
  const config = getConfig();

  if (config.OPENAI_API_KEY) {
    const openAIProvider = new OpenAIProvider({
      apiKey: config.OPENAI_API_KEY,
      llmModel: config.LLM_MODEL,
      embeddingModel: config.EMBEDDING_MODEL,
      dimensions: EMBEDDING_DIMENSIONS,
    });

    return {
      embeddingProvider: openAIProvider,
      llmProvider: openAIProvider,
      usingOpenAI: true,
    };
  }

  const fallback = new FallbackProvider(EMBEDDING_DIMENSIONS);
  return {
    embeddingProvider: fallback,
    llmProvider: fallback,
    usingOpenAI: false,
  };
}

export async function runPipeline(input: RunPipelineInput): Promise<PipelineRunSummary> {
  const config = getConfig();
  await runMigrations();

  const sourceRows = await readCatalogFile(input.inputPath);
  const deduplicated = deduplicateRows(sourceRows);
  const normalized = normalizeRows(deduplicated);

  if (normalized.length === 0) {
    throw new Error("No valid products found in the input file.");
  }

  const runId = await createPipelineRun({
    storeId: input.storeId,
    inputFileName: path.basename(input.inputPath),
    runLabel: input.runLabel,
  });

  try {
    const { embeddingProvider, llmProvider, usingOpenAI } = createProviders();
    if (!usingOpenAI) {
      // eslint-disable-next-line no-console
      console.warn(
        "OPENAI_API_KEY not found. Using deterministic fallback provider for category generation and vectors.",
      );
    }

    const { clusters, skuToClusterKey } = clusterProducts(normalized);
    const { drafts, clusterKeyToSlug } = await generateCategoryDrafts(clusters, llmProvider);
    const categoriesBySlug = await upsertCategoryDrafts(input.storeId, drafts);

    const clusterScoreBySku: Record<string, number> = {};
    for (const cluster of clusters) {
      for (const [sku, score] of Object.entries(cluster.scoresBySku)) {
        clusterScoreBySku[sku] = score;
      }
    }

    const limiter = pLimit(config.CONCURRENCY);
    const enrichmentMap = new Map<string, ProductEnrichment>();

    await Promise.all(
      normalized.map((product) =>
        limiter(async () => {
          const clusterKey = skuToClusterKey[product.sourceSku];
          const categorySlug = clusterKeyToSlug[clusterKey];
          const category = categoriesBySlug.get(categorySlug);

          if (!category) {
            enrichmentMap.set(product.sourceSku, {
              sourceSku: product.sourceSku,
              categorySlug: "",
              categoryConfidence: 0,
              attributeValues: {},
              attributeConfidence: {},
              needsReview: true,
              uncertaintyReasons: ["missing_category"],
            });
            return;
          }

          const enrichment = await enrichProduct(
            product,
            {
              slug: category.slug,
              attributes: category.attributes_jsonb,
              description: category.description_pt,
              confidenceScore: clusterScoreBySku[product.sourceSku] ?? 0,
            },
            llmProvider,
            config.CONFIDENCE_THRESHOLD,
          );

          enrichmentMap.set(product.sourceSku, enrichment);
        }),
      ),
    );

    const embeddingWorkItems: Array<{ sourceSku: string; text: string }> = [];
    const embeddedTextBySku = new Map<string, string>();

    for (const product of normalized) {
      const enrichment = enrichmentMap.get(product.sourceSku);
      if (!enrichment) {
        continue;
      }

      const category = categoriesBySlug.get(enrichment.categorySlug);
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
      embeddingProvider,
      config.BATCH_SIZE,
      config.CONCURRENCY,
    );

    const vectorsBySku = new Map<string, number[]>();
    for (const [sku, vector] of rawVectors.entries()) {
      vectorsBySku.set(sku, ensureVectorLength(vector, EMBEDDING_DIMENSIONS));
    }

    const persistedProducts = await upsertProducts({
      storeId: input.storeId,
      runId,
      products: normalized,
      enrichments: enrichmentMap,
      categoriesBySlug,
    });

    await upsertProductVectors({
      productBySku: persistedProducts,
      vectorsBySku,
      embeddedTextBySku,
      embeddingModel: config.EMBEDDING_MODEL,
    });

    const qaRows = normalized.map((product) => {
      const enrichment = enrichmentMap.get(product.sourceSku);
      const categoryName = enrichment
        ? categoriesBySlug.get(enrichment.categorySlug)?.name_pt ?? "sem_categoria"
        : "sem_categoria";

      return {
        sourceSku: product.sourceSku,
        title: product.title,
        predictedCategory: categoryName,
        needsReview: enrichment?.needsReview ?? true,
        attributeValues: enrichment?.attributeValues ?? {},
      };
    });

    const qaResult = await writeQAReport({
      outputDir: config.OUTPUT_DIR,
      runId,
      rows: qaRows,
      sampleSize: config.QA_SAMPLE_SIZE,
    });

    const needsReviewCount = qaRows.filter((row) => row.needsReview).length;

    await finalizePipelineRun({
      runId,
      status: "completed_pending_review",
      stats: {
        total_rows: sourceRows.length,
        unique_products: normalized.length,
        category_count: drafts.length,
        needs_review_count: needsReviewCount,
        qa_sampled_rows: qaResult.sampledRows,
        qa_total_rows: qaResult.totalRows,
        qa_report_path: qaResult.filePath,
      },
    });

    return {
      runId,
      storeId: input.storeId,
      inputFileName: path.basename(input.inputPath),
      totalRows: sourceRows.length,
      uniqueProducts: normalized.length,
      categoryCount: drafts.length,
      needsReviewCount,
      qaReportPath: qaResult.filePath,
      status: "completed_pending_review",
    };
  } catch (error) {
    await finalizePipelineRun({
      runId,
      status: "failed",
      stats: {
        error_message: error instanceof Error ? error.message : "unknown_error",
      },
    });

    throw error;
  }
}

export function __test_only_createProviders(): {
  embeddingProvider: EmbeddingProvider;
  llmProvider: LLMProvider;
  usingOpenAI: boolean;
} {
  return createProviders();
}

export function __test_only_ensureVectorLength(vector: number[], target: number): number[] {
  return ensureVectorLength(vector, target);
}

export function __test_only_normalizeRowsForPipeline(
  rows: Array<{ sourceSku: string; title: string; description?: string; brand?: string }>,
): NormalizedCatalogProduct[] {
  return normalizeRows(rows);
}

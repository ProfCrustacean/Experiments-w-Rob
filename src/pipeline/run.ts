import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import pLimit from "p-limit";
import { getConfig } from "../config.js";
import type {
  AttributeExtractionLLMOutput,
  EmbeddingProvider,
  LLMProvider,
  NormalizedCatalogProduct,
  PipelineRunSummary,
  ProductEnrichment,
} from "../types.js";
import { runMigrations } from "../db/migrate.js";
import { clusterProducts } from "./categorization.js";
import { buildEmbeddingText, generateEmbeddingsForItems } from "./embedding.js";
import { enrichProductWithSignals } from "./enrichment.js";
import { generateCategoryDrafts } from "./category-generation.js";
import { deduplicateRows, normalizeRows, readCatalogFile } from "./ingest.js";
import {
  cleanupExpiredRunArtifacts,
  createPipelineRun,
  finalizePipelineRun,
  upsertRunArtifact,
  upsertCategoryDrafts,
  upsertProducts,
  upsertProductVectors,
} from "./persist.js";
import { writeQAReport } from "./qa-report.js";
import { buildRunArtifacts } from "./run-artifacts.js";
import { FallbackProvider } from "../services/fallback.js";
import { OpenAIProvider } from "../services/openai.js";

const EMBEDDING_DIMENSIONS = 1536;

interface RunPipelineInput {
  inputPath: string;
  storeId: string;
  runLabel?: string;
  sampleParts?: number;
  samplePartIndex?: number;
}

interface CategoryContext {
  slug: string;
  attributes: {
    schema_version: "1.0";
    category_name_pt: string;
    attributes: Array<{
      key: string;
      label_pt: string;
      type: "enum" | "number" | "boolean" | "text";
      allowed_values?: string[];
      required: boolean;
    }>;
  };
  description: string;
  confidenceScore: number;
}

interface AttributeBatchTask {
  categorySlug: string;
  categoryContext: CategoryContext;
  items: NormalizedCatalogProduct[];
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

function hashString(value: string): number {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return Math.abs(hash >>> 0);
}

function shouldLogProgress(done: number, total: number): boolean {
  if (total <= 0) {
    return false;
  }
  if (done === total) {
    return true;
  }
  return done === 1 || done % 10 === 0;
}

function partitionProductsBySample(
  products: NormalizedCatalogProduct[],
  storeId: string,
  sampleParts: number,
  samplePartIndex: number,
): {
  sampled: NormalizedCatalogProduct[];
  skipped: number;
} {
  if (sampleParts <= 1) {
    return {
      sampled: products,
      skipped: 0,
    };
  }

  const sampled: NormalizedCatalogProduct[] = [];
  let skipped = 0;

  for (const product of products) {
    const partition = hashString(`${storeId}::${product.sourceSku}`) % sampleParts;
    if (partition === samplePartIndex) {
      sampled.push(product);
    } else {
      skipped += 1;
    }
  }

  return {
    sampled,
    skipped,
  };
}

function validateSamplingInput(sampleParts: number, samplePartIndex: number): void {
  if (!Number.isInteger(sampleParts) || sampleParts <= 0) {
    throw new Error(`Invalid sample-parts value: ${sampleParts}. Expected a positive integer.`);
  }
  if (!Number.isInteger(samplePartIndex) || samplePartIndex < 0) {
    throw new Error(
      `Invalid sample-part-index value: ${samplePartIndex}. Expected a non-negative integer.`,
    );
  }
  if (samplePartIndex >= sampleParts) {
    throw new Error(
      `Invalid sampling configuration: sample-part-index (${samplePartIndex}) must be smaller than sample-parts (${sampleParts}).`,
    );
  }
}

function createProviders(): {
  embeddingProvider: EmbeddingProvider;
  llmProvider: LLMProvider;
  usingOpenAI: boolean;
  openAIProvider: OpenAIProvider | null;
} {
  const config = getConfig();

  if (config.OPENAI_API_KEY) {
    const openAIProvider = new OpenAIProvider({
      apiKey: config.OPENAI_API_KEY,
      llmModel: config.LLM_MODEL,
      embeddingModel: config.EMBEDDING_MODEL,
      dimensions: EMBEDDING_DIMENSIONS,
      timeoutMs: config.OPENAI_TIMEOUT_MS,
      maxRetries: config.OPENAI_MAX_RETRIES,
      retryBaseMs: config.OPENAI_RETRY_BASE_MS,
      retryMaxMs: config.OPENAI_RETRY_MAX_MS,
    });

    return {
      embeddingProvider: openAIProvider,
      llmProvider: openAIProvider,
      usingOpenAI: true,
      openAIProvider,
    };
  }

  const fallback = new FallbackProvider(EMBEDDING_DIMENSIONS);
  return {
    embeddingProvider: fallback,
    llmProvider: fallback,
    usingOpenAI: false,
    openAIProvider: null,
  };
}

function buildMissingCategoryEnrichment(sourceSku: string): ProductEnrichment {
  return {
    sourceSku,
    categorySlug: "",
    categoryConfidence: 0,
    attributeValues: {},
    attributeConfidence: {},
    needsReview: true,
    uncertaintyReasons: ["missing_category"],
  };
}

export async function runPipeline(input: RunPipelineInput): Promise<PipelineRunSummary> {
  const config = getConfig();
  const sampleParts = input.sampleParts ?? config.INPUT_SAMPLE_PARTS;
  const samplePartIndex = input.samplePartIndex ?? config.INPUT_SAMPLE_PART_INDEX;
  validateSamplingInput(sampleParts, samplePartIndex);

  await runMigrations();

  const sourceRows = await readCatalogFile(input.inputPath);
  const deduplicated = deduplicateRows(sourceRows);
  const normalized = normalizeRows(deduplicated);

  if (normalized.length === 0) {
    throw new Error("No valid products found in the input file.");
  }

  const partitioned = partitionProductsBySample(
    normalized,
    input.storeId,
    sampleParts,
    samplePartIndex,
  );

  if (partitioned.sampled.length === 0) {
    throw new Error(
      `Sampling selected 0 products (sample_parts=${sampleParts}, sample_part_index=${samplePartIndex}).`,
    );
  }

  // eslint-disable-next-line no-console
  console.log(
    `[sampling] selected ${partitioned.sampled.length}/${normalized.length} unique products (sample_part_index=${samplePartIndex}, sample_parts=${sampleParts})`,
  );

  const stageStartAt = new Date();
  const stageTimingsMs: Record<string, number> = {};

  const runId = await createPipelineRun({
    storeId: input.storeId,
    inputFileName: path.basename(input.inputPath),
    runLabel: input.runLabel,
  });

  let attributeBatchFallbackProducts = 0;
  let attributeBatchFailureCount = 0;
  let attributeBatchCount = 0;

  try {
    const { embeddingProvider, llmProvider, usingOpenAI, openAIProvider } = createProviders();

    if (!usingOpenAI) {
      // eslint-disable-next-line no-console
      console.warn(
        "OPENAI_API_KEY not found. Using deterministic fallback provider for category generation and vectors.",
      );
    }

    const categorizationStart = Date.now();
    const { clusters, skuToClusterKey } = clusterProducts(partitioned.sampled);
    stageTimingsMs.categorization_ms = Date.now() - categorizationStart;

    const categoryGenerationStart = Date.now();
    const { drafts, clusterKeyToSlug } = await generateCategoryDrafts(
      clusters,
      llmProvider,
      config.CATEGORY_PROFILE_CONCURRENCY,
      (done, total) => {
        if (shouldLogProgress(done, total)) {
          // eslint-disable-next-line no-console
          console.log(`[category_generation] ${done}/${total} completed`);
        }
      },
    );
    stageTimingsMs.category_generation_ms = Date.now() - categoryGenerationStart;

    const categoryUpsertStart = Date.now();
    const categoriesBySlug = await upsertCategoryDrafts(input.storeId, drafts);
    stageTimingsMs.category_upsert_ms = Date.now() - categoryUpsertStart;

    const clusterScoreBySku: Record<string, number> = {};
    for (const cluster of clusters) {
      for (const [sku, score] of Object.entries(cluster.scoresBySku)) {
        clusterScoreBySku[sku] = score;
      }
    }

    const enrichmentStart = Date.now();
    const enrichmentMap = new Map<string, ProductEnrichment>();

    const batchedByCategory = new Map<string, AttributeBatchTask>();

    for (const product of partitioned.sampled) {
      const clusterKey = skuToClusterKey[product.sourceSku];
      const categorySlug = clusterKeyToSlug[clusterKey];
      const category = categoriesBySlug.get(categorySlug);

      if (!category) {
        enrichmentMap.set(product.sourceSku, buildMissingCategoryEnrichment(product.sourceSku));
        continue;
      }

      const context: CategoryContext = {
        slug: category.slug,
        attributes: category.attributes_jsonb,
        description: category.description_pt,
        confidenceScore: clusterScoreBySku[product.sourceSku] ?? 0,
      };

      if (!usingOpenAI) {
        const enrichment = enrichProductWithSignals(
          product,
          context,
          null,
          config.CONFIDENCE_THRESHOLD,
        );
        enrichmentMap.set(product.sourceSku, enrichment);
        continue;
      }

      const existingTask = batchedByCategory.get(category.slug);
      if (existingTask) {
        existingTask.items.push(product);
      } else {
        batchedByCategory.set(category.slug, {
          categorySlug: category.slug,
          categoryContext: context,
          items: [product],
        });
      }
    }

    if (usingOpenAI) {
      const batchTasks: AttributeBatchTask[] = [];
      for (const task of batchedByCategory.values()) {
        for (let index = 0; index < task.items.length; index += config.ATTRIBUTE_BATCH_SIZE) {
          batchTasks.push({
            categorySlug: task.categorySlug,
            categoryContext: task.categoryContext,
            items: task.items.slice(index, index + config.ATTRIBUTE_BATCH_SIZE),
          });
        }
      }

      attributeBatchCount = batchTasks.length;
      let completedBatches = 0;
      const limiter = pLimit(config.ATTRIBUTE_LLM_CONCURRENCY);

      // eslint-disable-next-line no-console
      console.log(
        `[attribute_batches] ${batchTasks.length} batch requests (batch_size=${config.ATTRIBUTE_BATCH_SIZE}, concurrency=${config.ATTRIBUTE_LLM_CONCURRENCY})`,
      );

      await Promise.all(
        batchTasks.map((task) =>
          limiter(async () => {
            let outputBySku: Record<string, AttributeExtractionLLMOutput> | null = null;

            try {
              outputBySku = await llmProvider.extractProductAttributesBatch({
                categoryName: task.categoryContext.attributes.category_name_pt,
                categoryDescription: task.categoryContext.description,
                attributeSchema: task.categoryContext.attributes,
                products: task.items.map((product) => ({
                  sourceSku: product.sourceSku,
                  product: {
                    title: product.title,
                    description: product.description,
                    brand: product.brand,
                  },
                })),
              });
            } catch {
              attributeBatchFailureCount += 1;
              outputBySku = null;
            }

            for (const product of task.items) {
              const llmOutput = outputBySku?.[product.sourceSku] ?? null;
              const fallbackReason = llmOutput
                ? undefined
                : outputBySku
                  ? "llm_output_missing"
                  : "llm_batch_fallback";

              if (fallbackReason) {
                attributeBatchFallbackProducts += 1;
              }

              const enrichment = enrichProductWithSignals(
                product,
                task.categoryContext,
                llmOutput,
                config.CONFIDENCE_THRESHOLD,
                fallbackReason ? { fallbackReason } : undefined,
              );

              enrichmentMap.set(product.sourceSku, enrichment);
            }

            completedBatches += 1;
            if (shouldLogProgress(completedBatches, batchTasks.length)) {
              // eslint-disable-next-line no-console
              console.log(`[attribute_batches] ${completedBatches}/${batchTasks.length} completed`);
            }
          }),
        ),
      );
    }

    for (const product of partitioned.sampled) {
      if (!enrichmentMap.has(product.sourceSku)) {
        enrichmentMap.set(product.sourceSku, buildMissingCategoryEnrichment(product.sourceSku));
      }
    }

    stageTimingsMs.enrichment_ms = Date.now() - enrichmentStart;

    const embeddingStart = Date.now();
    const embeddingWorkItems: Array<{ sourceSku: string; text: string }> = [];
    const embeddedTextBySku = new Map<string, string>();

    for (const product of partitioned.sampled) {
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
      config.EMBEDDING_BATCH_SIZE,
      config.EMBEDDING_CONCURRENCY,
      (done, total) => {
        if (shouldLogProgress(done, total)) {
          // eslint-disable-next-line no-console
          console.log(`[embeddings] ${done}/${total} batches completed`);
        }
      },
    );

    const vectorsBySku = new Map<string, number[]>();
    for (const [sku, vector] of rawVectors.entries()) {
      vectorsBySku.set(sku, ensureVectorLength(vector, EMBEDDING_DIMENSIONS));
    }
    stageTimingsMs.embedding_ms = Date.now() - embeddingStart;

    const productPersistStart = Date.now();
    const persistedProducts = await upsertProducts({
      storeId: input.storeId,
      runId,
      products: partitioned.sampled,
      enrichments: enrichmentMap,
      categoriesBySlug,
    });

    await upsertProductVectors({
      productBySku: persistedProducts,
      vectorsBySku,
      embeddedTextBySku,
      embeddingModel: config.EMBEDDING_MODEL,
    });
    stageTimingsMs.product_persist_ms = Date.now() - productPersistStart;

    const qaStart = Date.now();
    const qaRows = partitioned.sampled.map((product) => {
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
    stageTimingsMs.qa_report_ms = Date.now() - qaStart;

    const needsReviewCount = qaRows.filter((row) => row.needsReview).length;

    const openAIStats = usingOpenAI ? openAIProvider?.getStats() ?? null : null;
    const runFinishedAt = new Date();
    const artifactsExpireAt = new Date(
      runFinishedAt.getTime() + config.ARTIFACT_RETENTION_HOURS * 60 * 60 * 1000,
    );

    const artifactBuildStart = Date.now();
    const artifactBuild = buildRunArtifacts({
      runId,
      storeId: input.storeId,
      inputFileName: path.basename(input.inputPath),
      products: partitioned.sampled,
      enrichments: enrichmentMap,
      categoriesBySlug,
      qaReportFileName: qaResult.fileName,
      qaReportCsvContent: qaResult.csvContent,
      categoryCount: drafts.length,
      needsReviewCount,
      stageTimingsMs,
      openAIEnabled: usingOpenAI,
      openAIRequestStats: openAIStats,
      attributeBatchFailureCount,
      attributeBatchFallbackProducts,
      startedAt: stageStartAt,
      finishedAt: runFinishedAt,
      expiresAt: artifactsExpireAt,
    });
    stageTimingsMs.artifact_generation_ms = Date.now() - artifactBuildStart;

    const artifactPersistStart = Date.now();
    await mkdir(config.OUTPUT_DIR, { recursive: true });

    for (const artifact of artifactBuild.artifacts) {
      const localPath = path.join(config.OUTPUT_DIR, artifact.fileName);
      await writeFile(localPath, artifact.content);
      await upsertRunArtifact({
        runId,
        artifactKey: artifact.key,
        fileName: artifact.fileName,
        mimeType: artifact.mimeType,
        content: artifact.content,
        expiresAt: artifactsExpireAt,
      });
    }
    stageTimingsMs.artifact_persist_ms = Date.now() - artifactPersistStart;

    const artifactCleanupStart = Date.now();
    const cleanedArtifacts = await cleanupExpiredRunArtifacts(config.ARTIFACT_RETENTION_HOURS);
    stageTimingsMs.artifact_cleanup_ms = Date.now() - artifactCleanupStart;

    await finalizePipelineRun({
      runId,
      status: "completed_pending_review",
      stats: {
        total_rows: sourceRows.length,
        unique_products_input: normalized.length,
        unique_products_processed: partitioned.sampled.length,
        unique_products_skipped: partitioned.skipped,
        sample_parts: sampleParts,
        sample_part_index: samplePartIndex,
        category_count: drafts.length,
        needs_review_count: needsReviewCount,
        qa_sampled_rows: qaResult.sampledRows,
        qa_total_rows: qaResult.totalRows,
        qa_report_path: qaResult.filePath,
        artifact_retention_hours: config.ARTIFACT_RETENTION_HOURS,
        artifacts: artifactBuild.artifactSummaries,
        artifact_cleanup_deleted_count: cleanedArtifacts,
        openai_enabled: usingOpenAI,
        attribute_batch_count: attributeBatchCount,
        attribute_batch_failure_count: attributeBatchFailureCount,
        attribute_batch_fallback_products: attributeBatchFallbackProducts,
        stage_timings_ms: stageTimingsMs,
        openai_request_stats: openAIStats,
      },
    });

    // eslint-disable-next-line no-console
    console.log(
      `[run_summary] completed run ${runId} with ${partitioned.sampled.length} products in ${Math.round((Date.now() - stageStartAt.getTime()) / 1000)}s`,
    );

    return {
      runId,
      storeId: input.storeId,
      inputFileName: path.basename(input.inputPath),
      totalRows: sourceRows.length,
      uniqueProducts: partitioned.sampled.length,
      categoryCount: drafts.length,
      needsReviewCount,
      qaReportPath: qaResult.filePath,
      artifacts: artifactBuild.artifactSummaries,
      status: "completed_pending_review",
    };
  } catch (error) {
    await finalizePipelineRun({
      runId,
      status: "failed",
      stats: {
        error_message: error instanceof Error ? error.message : "unknown_error",
        stage_timings_ms: stageTimingsMs,
        sample_parts: sampleParts,
        sample_part_index: samplePartIndex,
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
  const providers = createProviders();
  return {
    embeddingProvider: providers.embeddingProvider,
    llmProvider: providers.llmProvider,
    usingOpenAI: providers.usingOpenAI,
  };
}

export function __test_only_ensureVectorLength(vector: number[], target: number): number[] {
  return ensureVectorLength(vector, target);
}

export function __test_only_normalizeRowsForPipeline(
  rows: Array<{ sourceSku: string; title: string; description?: string; brand?: string }>,
): NormalizedCatalogProduct[] {
  return normalizeRows(rows);
}

export function __test_only_partitionProductsBySample(
  products: NormalizedCatalogProduct[],
  storeId: string,
  sampleParts: number,
  samplePartIndex: number,
): {
  sampled: NormalizedCatalogProduct[];
  skipped: number;
} {
  return partitionProductsBySample(products, storeId, sampleParts, samplePartIndex);
}

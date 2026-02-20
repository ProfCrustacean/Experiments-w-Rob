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
  cleanupExpiredRunLogs,
  createPipelineRun,
  finalizePipelineRun,
  insertRunLogBatch,
  upsertRunArtifact,
  upsertCategoryDrafts,
  upsertProducts,
  upsertProductVectors,
} from "./persist.js";
import { writeQAReport } from "./qa-report.js";
import { buildRunArtifacts } from "./run-artifacts.js";
import { FallbackProvider } from "../services/fallback.js";
import { OpenAIProvider, type OpenAITelemetryCallback } from "../services/openai.js";
import { RunLogger } from "../logging/run-logger.js";

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

function createProviders(openAITelemetry?: OpenAITelemetryCallback): {
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
      telemetry: openAITelemetry,
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

  const ingestStart = Date.now();
  const sourceRows = await readCatalogFile(input.inputPath);
  const deduplicated = deduplicateRows(sourceRows);
  const normalized = normalizeRows(deduplicated);
  const ingestElapsedMs = Date.now() - ingestStart;

  if (normalized.length === 0) {
    throw new Error("No valid products found in the input file.");
  }

  const samplingStart = Date.now();
  const partitioned = partitionProductsBySample(
    normalized,
    input.storeId,
    sampleParts,
    samplePartIndex,
  );
  const samplingElapsedMs = Date.now() - samplingStart;

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
  stageTimingsMs.ingest_ms = ingestElapsedMs;
  stageTimingsMs.sampling_ms = samplingElapsedMs;

  const runId = await createPipelineRun({
    storeId: input.storeId,
    inputFileName: path.basename(input.inputPath),
    runLabel: input.runLabel,
  });

  let attributeBatchFallbackProducts = 0;
  let attributeBatchFailureCount = 0;
  let attributeBatchCount = 0;
  const logger = new RunLogger({
    runId,
    traceRetentionHours: config.TRACE_RETENTION_HOURS,
    flushBatchSize: config.TRACE_FLUSH_BATCH_SIZE,
    insertBatch: insertRunLogBatch,
  });

  try {
    if (!config.OPENAI_API_KEY) {
      throw new Error(
        "OPENAI_API_KEY is missing. OpenAI mode is required for this pipeline run.",
      );
    }

    const configSnapshot = {
      llm_model: config.LLM_MODEL,
      embedding_model: config.EMBEDDING_MODEL,
      confidence_threshold: config.CONFIDENCE_THRESHOLD,
      category_profile_concurrency: config.CATEGORY_PROFILE_CONCURRENCY,
      attribute_batch_size: config.ATTRIBUTE_BATCH_SIZE,
      attribute_llm_concurrency: config.ATTRIBUTE_LLM_CONCURRENCY,
      embedding_batch_size: config.EMBEDDING_BATCH_SIZE,
      embedding_concurrency: config.EMBEDDING_CONCURRENCY,
      openai_timeout_ms: config.OPENAI_TIMEOUT_MS,
      openai_max_retries: config.OPENAI_MAX_RETRIES,
      openai_retry_base_ms: config.OPENAI_RETRY_BASE_MS,
      openai_retry_max_ms: config.OPENAI_RETRY_MAX_MS,
      trace_retention_hours: config.TRACE_RETENTION_HOURS,
      trace_flush_batch_size: config.TRACE_FLUSH_BATCH_SIZE,
      output_dir: config.OUTPUT_DIR,
    };

    logger.info("pipeline", "run.started", "Pipeline run started.", {
      run_id: runId,
      store_id: input.storeId,
      input_file_name: path.basename(input.inputPath),
      sample_parts: sampleParts,
      sample_part_index: samplePartIndex,
      openai_enabled: true,
      config: configSnapshot,
    });

    logger.info("pipeline", "stage.started", "Starting ingest stage.", {
      stage_name: "ingest",
      historical_stage: true,
    });
    logger.info("pipeline", "stage.completed", "Ingest stage completed.", {
      stage_name: "ingest",
      historical_stage: true,
      elapsed_ms: ingestElapsedMs,
      input_rows: sourceRows.length,
      deduplicated_rows: deduplicated.length,
      normalized_rows: normalized.length,
    });

    logger.info("pipeline", "stage.started", "Starting sampling stage.", {
      stage_name: "sampling",
      historical_stage: true,
    });
    logger.info("pipeline", "stage.completed", "Sampling stage completed.", {
      stage_name: "sampling",
      historical_stage: true,
      elapsed_ms: samplingElapsedMs,
      sampled_rows: partitioned.sampled.length,
      skipped_rows: partitioned.skipped,
    });

    const openAITelemetry: OpenAITelemetryCallback = (event) => {
      logger.log(event.level, event.stage, event.event, event.message, event.payload);
    };

    const { embeddingProvider, llmProvider, usingOpenAI, openAIProvider } = createProviders(
      openAITelemetry,
    );

    logger.info("pipeline", "stage.started", "Starting categorization stage.", {
      stage_name: "categorization",
      product_count: partitioned.sampled.length,
    });
    const categorizationStart = Date.now();
    const { clusters, skuToClusterKey } = clusterProducts(partitioned.sampled);
    stageTimingsMs.categorization_ms = Date.now() - categorizationStart;
    logger.info("pipeline", "stage.completed", "Categorization stage completed.", {
      stage_name: "categorization",
      elapsed_ms: stageTimingsMs.categorization_ms,
      cluster_count: clusters.length,
    });

    logger.info("pipeline", "stage.started", "Starting category generation stage.", {
      stage_name: "category_generation",
      cluster_count: clusters.length,
    });
    const categoryGenerationStart = Date.now();
    const { drafts, clusterKeyToSlug } = await generateCategoryDrafts(
      clusters,
      llmProvider,
      config.CATEGORY_PROFILE_CONCURRENCY,
      (done, total) => {
        if (shouldLogProgress(done, total)) {
          // eslint-disable-next-line no-console
          console.log(`[category_generation] ${done}/${total} completed`);
          logger.info(
            "pipeline",
            "batch.category_profile.completed",
            "Category profile batch progress updated.",
            {
              completed: done,
              total,
            },
          );
        }
      },
    );
    stageTimingsMs.category_generation_ms = Date.now() - categoryGenerationStart;
    logger.info("pipeline", "stage.completed", "Category generation stage completed.", {
      stage_name: "category_generation",
      elapsed_ms: stageTimingsMs.category_generation_ms,
      category_count: drafts.length,
    });

    logger.info("pipeline", "stage.started", "Starting category persistence stage.", {
      stage_name: "category_persist",
      category_count: drafts.length,
    });
    const categoryUpsertStart = Date.now();
    const categoriesBySlug = await upsertCategoryDrafts(input.storeId, drafts);
    stageTimingsMs.category_upsert_ms = Date.now() - categoryUpsertStart;
    logger.info("pipeline", "stage.completed", "Category persistence stage completed.", {
      stage_name: "category_persist",
      elapsed_ms: stageTimingsMs.category_upsert_ms,
    });

    const clusterScoreBySku: Record<string, number> = {};
    for (const cluster of clusters) {
      for (const [sku, score] of Object.entries(cluster.scoresBySku)) {
        clusterScoreBySku[sku] = score;
      }
    }

    logger.info("pipeline", "stage.started", "Starting enrichment stage.", {
      stage_name: "enrichment",
    });
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
            logger.debug("pipeline", "batch.attribute.started", "Attribute batch started.", {
              category_slug: task.categorySlug,
              sku_count: task.items.length,
              source_skus: task.items.map((item) => item.sourceSku),
            });

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
            } catch (error) {
              attributeBatchFailureCount += 1;
              outputBySku = null;
              logger.warn("pipeline", "batch.attribute.failed", "Attribute batch failed.", {
                category_slug: task.categorySlug,
                sku_count: task.items.length,
                error_message: error instanceof Error ? error.message : "unknown_error",
              });
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
            logger.info("pipeline", "batch.attribute.completed", "Attribute batch completed.", {
              category_slug: task.categorySlug,
              completed_batches: completedBatches,
              total_batches: batchTasks.length,
            });
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
    logger.info("pipeline", "stage.completed", "Enrichment stage completed.", {
      stage_name: "enrichment",
      elapsed_ms: stageTimingsMs.enrichment_ms,
      attribute_batch_count: attributeBatchCount,
      attribute_batch_failure_count: attributeBatchFailureCount,
      attribute_batch_fallback_products: attributeBatchFallbackProducts,
    });

    logger.info("pipeline", "stage.started", "Starting embedding stage.", {
      stage_name: "embedding",
    });
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
          logger.info(
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
    stageTimingsMs.embedding_ms = Date.now() - embeddingStart;
    logger.info("pipeline", "stage.completed", "Embedding stage completed.", {
      stage_name: "embedding",
      elapsed_ms: stageTimingsMs.embedding_ms,
      embedded_products: vectorsBySku.size,
    });

    logger.info("pipeline", "stage.started", "Starting product persistence stage.", {
      stage_name: "product_persist",
    });
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
    logger.info("pipeline", "stage.completed", "Product persistence stage completed.", {
      stage_name: "product_persist",
      elapsed_ms: stageTimingsMs.product_persist_ms,
      persisted_products: persistedProducts.size,
    });

    logger.info("pipeline", "stage.started", "Starting QA report stage.", {
      stage_name: "qa_report",
    });
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
    logger.info("pipeline", "stage.completed", "QA report stage completed.", {
      stage_name: "qa_report",
      elapsed_ms: stageTimingsMs.qa_report_ms,
      qa_sampled_rows: qaResult.sampledRows,
      qa_total_rows: qaResult.totalRows,
    });

    const needsReviewCount = qaRows.filter((row) => row.needsReview).length;

    const openAIStats = usingOpenAI ? openAIProvider?.getStats() ?? null : null;
    const runFinishedAt = new Date();
    const artifactsExpireAt = new Date(
      runFinishedAt.getTime() + config.ARTIFACT_RETENTION_HOURS * 60 * 60 * 1000,
    );

    logger.info("pipeline", "stage.started", "Starting artifact generation stage.", {
      stage_name: "artifact_generation",
    });
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
    logger.info("pipeline", "stage.completed", "Artifact generation stage completed.", {
      stage_name: "artifact_generation",
      elapsed_ms: stageTimingsMs.artifact_generation_ms,
      artifact_count: artifactBuild.artifacts.length,
    });

    logger.info("pipeline", "stage.started", "Starting artifact persistence stage.", {
      stage_name: "artifact_persist",
    });
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
    logger.info("pipeline", "stage.completed", "Artifact persistence stage completed.", {
      stage_name: "artifact_persist",
      elapsed_ms: stageTimingsMs.artifact_persist_ms,
      artifact_count: artifactBuild.artifacts.length,
    });

    logger.info("pipeline", "stage.started", "Starting cleanup stage.", {
      stage_name: "cleanup",
    });
    const artifactCleanupStart = Date.now();
    const cleanedArtifacts = await cleanupExpiredRunArtifacts(config.ARTIFACT_RETENTION_HOURS);
    const cleanedLogs = await cleanupExpiredRunLogs(config.TRACE_RETENTION_HOURS);
    stageTimingsMs.artifact_cleanup_ms = Date.now() - artifactCleanupStart;
    logger.info("persistence", "cleanup.logs.completed", "Expired trace logs cleanup completed.", {
      deleted_count: cleanedLogs,
      retention_hours: config.TRACE_RETENTION_HOURS,
    });
    logger.info("pipeline", "stage.completed", "Cleanup stage completed.", {
      stage_name: "cleanup",
      elapsed_ms: stageTimingsMs.artifact_cleanup_ms,
      artifact_cleanup_deleted_count: cleanedArtifacts,
      log_cleanup_deleted_count: cleanedLogs,
    });

    logger.info("pipeline", "run.completed", "Pipeline run completed successfully.", {
      run_id: runId,
      processed_products: partitioned.sampled.length,
      category_count: drafts.length,
      needs_review_count: needsReviewCount,
    });

    await logger.flush("run_completed");
    const traceStats = logger.getStats();

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
        trace_cleanup_deleted_count: cleanedLogs,
        trace_retention_hours: config.TRACE_RETENTION_HOURS,
        trace_event_count: traceStats.trace_event_count,
        trace_openai_event_count: traceStats.trace_openai_event_count,
        trace_flush_error_count: traceStats.trace_flush_error_count,
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
    logger.error("pipeline", "run.failed", "Pipeline run failed.", {
      run_id: runId,
      error_message: error instanceof Error ? error.message : "unknown_error",
    });

    await logger.flush("run_failed");
    const traceStats = logger.getStats();

    await finalizePipelineRun({
      runId,
      status: "failed",
      stats: {
        error_message: error instanceof Error ? error.message : "unknown_error",
        stage_timings_ms: stageTimingsMs,
        sample_parts: sampleParts,
        sample_part_index: samplePartIndex,
        trace_retention_hours: config.TRACE_RETENTION_HOURS,
        trace_event_count: traceStats.trace_event_count,
        trace_openai_event_count: traceStats.trace_openai_event_count,
        trace_flush_error_count: traceStats.trace_flush_error_count,
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

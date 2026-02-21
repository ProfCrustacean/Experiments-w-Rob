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
import { assignCategoriesForProducts } from "./category-assignment.js";
import { buildConfusionHotlist } from "./confusion-hotlist.js";
import { buildEmbeddingText, generateEmbeddingsForItems } from "./embedding.js";
import { enrichProductWithSignals } from "./enrichment.js";
import { buildCategoryDraftsFromTaxonomyAssignments } from "./taxonomy-category-drafts.js";
import { deduplicateRows, normalizeRows, readCatalogFile } from "./ingest.js";
import {
  cleanupExpiredRunArtifacts,
  cleanupExpiredRunLogs,
  createPipelineRun,
  finalizePipelineRun,
  insertRunLogBatch,
  recoverStaleRunningRuns,
  upsertRunArtifact,
  upsertCategoryDrafts,
  upsertProducts,
  upsertProductVectors,
} from "./persist.js";
import { writeQAReport } from "./qa-report.js";
import { buildRunArtifacts } from "./run-artifacts.js";
import { buildVariantSignature, deriveLegacySplitHint } from "./variant-signature.js";
import { FallbackProvider } from "../services/fallback.js";
import { OpenAIProvider, type OpenAITelemetryCallback } from "../services/openai.js";
import { RunLogger } from "../logging/run-logger.js";
import { loadTaxonomy } from "../taxonomy/load.js";

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
  top2Confidence: number;
  margin: number;
  autoDecision: "auto" | "review";
  confidenceReasons: string[];
  isFallbackCategory: boolean;
  contradictionCount: number;
}

interface AttributeBatchItem {
  product: NormalizedCatalogProduct;
  context: CategoryContext;
}

interface AttributeBatchTask {
  categorySlug: string;
  categoryContext: Pick<CategoryContext, "slug" | "description">;
  llmAttributeSchema: CategoryContext["attributes"];
  items: AttributeBatchItem[];
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

function hasVariantValues(values: Record<string, string | number | boolean | null>): boolean {
  return Object.entries(values).some(([key, value]) => {
    if (key === "item_subtype") {
      return false;
    }
    return value !== null && value !== "";
  });
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
    categoryTop2Confidence: 0,
    categoryMargin: 0,
    autoDecision: "review",
    confidenceReasons: ["missing_category"],
    isFallbackCategory: true,
    categoryContradictionCount: 0,
    attributeValidationFailCount: 0,
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
  const staleRunsRecovered = await recoverStaleRunningRuns({
    storeId: input.storeId,
    staleAfterMinutes: config.STALE_RUN_TIMEOUT_MINUTES,
  });
  if (staleRunsRecovered > 0) {
    // eslint-disable-next-line no-console
    console.warn(
      `[stale_runs] recovered ${staleRunsRecovered} stale running run(s) older than ${config.STALE_RUN_TIMEOUT_MINUTES} minutes for store ${input.storeId}`,
    );
  }

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
      category_auto_min_confidence: config.CATEGORY_AUTO_MIN_CONFIDENCE,
      category_auto_min_margin: config.CATEGORY_AUTO_MIN_MARGIN,
      attribute_auto_min_confidence: config.ATTRIBUTE_AUTO_MIN_CONFIDENCE,
      high_risk_category_extra_confidence: config.HIGH_RISK_CATEGORY_EXTRA_CONFIDENCE,
      quality_qa_sample_size: config.QUALITY_QA_SAMPLE_SIZE,
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
    const categoryAssignments = await assignCategoriesForProducts({
      products: partitioned.sampled,
      embeddingProvider,
      llmProvider,
      autoMinConfidence: config.CATEGORY_AUTO_MIN_CONFIDENCE,
      autoMinMargin: config.CATEGORY_AUTO_MIN_MARGIN,
      highRiskExtraConfidence: config.HIGH_RISK_CATEGORY_EXTRA_CONFIDENCE,
      llmConcurrency: config.CATEGORY_PROFILE_CONCURRENCY,
      embeddingBatchSize: config.EMBEDDING_BATCH_SIZE,
      embeddingConcurrency: config.EMBEDDING_CONCURRENCY,
    });
    const taxonomy = loadTaxonomy();
    const assignedCategoryBySku = new Map<string, string>();
    for (const [sourceSku, assignment] of categoryAssignments.assignmentsBySku.entries()) {
      assignedCategoryBySku.set(sourceSku, assignment.categorySlug);
    }
    const assignedCategoryCount = new Set(assignedCategoryBySku.values()).size;
    stageTimingsMs.categorization_ms = Date.now() - categorizationStart;
    logger.info("pipeline", "stage.completed", "Categorization stage completed.", {
      stage_name: "categorization",
      elapsed_ms: stageTimingsMs.categorization_ms,
      category_count: assignedCategoryCount,
      confidence_histogram: categoryAssignments.confidenceHistogram,
    });

    logger.info("pipeline", "stage.started", "Starting category generation stage.", {
      stage_name: "category_generation",
      category_count: assignedCategoryCount,
    });
    const categoryGenerationStart = Date.now();
    const drafts = buildCategoryDraftsFromTaxonomyAssignments({
      assignedCategoryBySku,
    });
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

    logger.info("pipeline", "stage.started", "Starting enrichment stage.", {
      stage_name: "enrichment",
    });
    const enrichmentStart = Date.now();
    const enrichmentMap = new Map<string, ProductEnrichment>();

    const batchedByCategory = new Map<string, AttributeBatchTask>();

    for (const product of partitioned.sampled) {
      const assignment = categoryAssignments.assignmentsBySku.get(product.sourceSku);
      const categorySlug = assignment?.categorySlug ?? taxonomy.fallbackCategory.slug;
      const category = categoriesBySlug.get(categorySlug);

      if (!category) {
        enrichmentMap.set(product.sourceSku, buildMissingCategoryEnrichment(product.sourceSku));
        continue;
      }

      const context: CategoryContext = {
        slug: category.slug,
        attributes: category.attributes_jsonb,
        description: category.description_pt,
        confidenceScore: assignment?.categoryConfidence ?? 0,
        top2Confidence: assignment?.categoryTop2Confidence ?? 0,
        margin: assignment?.categoryMargin ?? 0,
        autoDecision: assignment?.autoDecision ?? "review",
        confidenceReasons: assignment?.confidenceReasons ?? ["missing_assignment"],
        isFallbackCategory: assignment?.isFallbackCategory ?? true,
        contradictionCount: assignment?.categoryContradictionCount ?? 0,
      };

      const ruleOnlyEnrichment = enrichProductWithSignals(
        product,
        context,
        null,
        config.CONFIDENCE_THRESHOLD,
        config.ATTRIBUTE_AUTO_MIN_CONFIDENCE,
      );

      if (!usingOpenAI) {
        enrichmentMap.set(product.sourceSku, ruleOnlyEnrichment);
        continue;
      }

      if (context.isFallbackCategory) {
        enrichmentMap.set(product.sourceSku, ruleOnlyEnrichment);
        continue;
      }

      const missingKeys = context.attributes.attributes
        .filter((attribute) => {
          const value = ruleOnlyEnrichment.attributeValues[attribute.key];
          const confidence = ruleOnlyEnrichment.attributeConfidence[attribute.key] ?? 0;
          if (value === null || value === "") {
            return true;
          }
          return attribute.required && confidence < config.ATTRIBUTE_AUTO_MIN_CONFIDENCE;
        })
        .map((attribute) => attribute.key);

      if (missingKeys.length === 0) {
        enrichmentMap.set(product.sourceSku, ruleOnlyEnrichment);
        continue;
      }

      const sortedMissingKeys = [...new Set(missingKeys)].sort();
      const taskKey = `${category.slug}::${sortedMissingKeys.join(",")}`;
      const llmAttributeSchema: CategoryContext["attributes"] = {
        ...context.attributes,
        attributes: context.attributes.attributes.filter((attribute) =>
          sortedMissingKeys.includes(attribute.key),
        ),
      };

      const existingTask = batchedByCategory.get(taskKey);
      if (existingTask) {
        existingTask.items.push({
          product,
          context,
        });
      } else {
        batchedByCategory.set(taskKey, {
          categorySlug: category.slug,
          categoryContext: {
            slug: context.slug,
            description: context.description,
          },
          llmAttributeSchema,
          items: [
            {
              product,
              context,
            },
          ],
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
            llmAttributeSchema: task.llmAttributeSchema,
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
              attribute_keys: task.llmAttributeSchema.attributes.map((attribute) => attribute.key),
              source_skus: task.items.map((item) => item.product.sourceSku),
            });

            let outputBySku: Record<string, AttributeExtractionLLMOutput> | null = null;

            try {
              outputBySku = await llmProvider.extractProductAttributesBatch({
                categoryName: task.llmAttributeSchema.category_name_pt,
                categoryDescription: task.categoryContext.description,
                attributeSchema: task.llmAttributeSchema,
                products: task.items.map(({ product }) => ({
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

            for (const item of task.items) {
              const { product, context } = item;
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
                context,
                llmOutput,
                config.CONFIDENCE_THRESHOLD,
                config.ATTRIBUTE_AUTO_MIN_CONFIDENCE,
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
      const categorySlug = enrichment?.categorySlug ?? "sem_categoria";
      const attributeValues = enrichment?.attributeValues ?? {};

      return {
        sourceSku: product.sourceSku,
        title: product.title,
        predictedCategory: categorySlug,
        predictedCategoryConfidence: enrichment?.categoryConfidence ?? 0,
        predictedCategoryMargin: enrichment?.categoryMargin ?? 0,
        autoDecision: enrichment?.autoDecision ?? "review",
        topConfidenceReasons: enrichment?.confidenceReasons ?? [],
        needsReview: enrichment?.needsReview ?? true,
        variantSignature: buildVariantSignature(attributeValues),
        legacySplitHint: deriveLegacySplitHint(categorySlug, attributeValues),
        attributeValues,
      };
    });

    const qaResult = await writeQAReport({
      outputDir: config.OUTPUT_DIR,
      runId,
      rows: qaRows,
      sampleSize: config.QUALITY_QA_SAMPLE_SIZE,
    });
    stageTimingsMs.qa_report_ms = Date.now() - qaStart;
    logger.info("pipeline", "stage.completed", "QA report stage completed.", {
      stage_name: "qa_report",
      elapsed_ms: stageTimingsMs.qa_report_ms,
      qa_sampled_rows: qaResult.sampledRows,
      qa_total_rows: qaResult.totalRows,
    });

    logger.info("pipeline", "stage.started", "Starting confusion hotlist stage.", {
      stage_name: "confusion_hotlist",
    });
    const confusionHotlistStart = Date.now();
    const confusionHotlist = buildConfusionHotlist({
      products: partitioned.sampled,
      assignmentsBySku: categoryAssignments.assignmentsBySku,
      maxRows: 20,
    });
    await mkdir(config.OUTPUT_DIR, { recursive: true });
    const confusionHotlistFileName = `confusion_hotlist_${runId}.csv`;
    const confusionHotlistPath = path.join(config.OUTPUT_DIR, confusionHotlistFileName);
    await writeFile(confusionHotlistPath, confusionHotlist.csvContent, "utf8");
    stageTimingsMs.confusion_hotlist_ms = Date.now() - confusionHotlistStart;
    logger.info("pipeline", "stage.completed", "Confusion hotlist stage completed.", {
      stage_name: "confusion_hotlist",
      elapsed_ms: stageTimingsMs.confusion_hotlist_ms,
      pair_count: confusionHotlist.rows.length,
      file_name: confusionHotlistFileName,
    });

    const needsReviewCount = qaRows.filter((row) => row.needsReview).length;
    const autoAcceptedCount = qaRows.filter((row) => row.autoDecision === "auto").length;
    const fallbackCategoryCount = [...enrichmentMap.values()].filter(
      (enrichment) => enrichment.isFallbackCategory,
    ).length;
    const categoryContradictionCount = [...enrichmentMap.values()].reduce(
      (sum, enrichment) => sum + enrichment.categoryContradictionCount,
      0,
    );
    const attributeValidationFailCount = [...enrichmentMap.values()].reduce(
      (sum, enrichment) => sum + enrichment.attributeValidationFailCount,
      0,
    );
    const processedCount = partitioned.sampled.length;
    const autoAcceptedRate = processedCount === 0 ? 0 : autoAcceptedCount / processedCount;
    const fallbackCategoryRate = processedCount === 0 ? 0 : fallbackCategoryCount / processedCount;
    const needsReviewRate = processedCount === 0 ? 0 : needsReviewCount / processedCount;
    const attributeValidationFailRate =
      processedCount === 0 ? 0 : attributeValidationFailCount / processedCount;
    const familyDistribution: Record<string, number> = {};
    const familyNeedsReviewCount: Record<string, number> = {};
    const variantFillCountByFamily: Record<string, number> = {};
    let variantFilledCount = 0;

    for (const enrichment of enrichmentMap.values()) {
      const family = enrichment.categorySlug || "sem_categoria";
      familyDistribution[family] = (familyDistribution[family] ?? 0) + 1;
      if (enrichment.needsReview) {
        familyNeedsReviewCount[family] = (familyNeedsReviewCount[family] ?? 0) + 1;
      }

      if (hasVariantValues(enrichment.attributeValues)) {
        variantFilledCount += 1;
        variantFillCountByFamily[family] = (variantFillCountByFamily[family] ?? 0) + 1;
      }
    }

    const familyReviewRate: Record<string, number> = {};
    const variantFillRateByFamily: Record<string, number> = {};
    for (const [family, familyCount] of Object.entries(familyDistribution)) {
      const reviewCount = familyNeedsReviewCount[family] ?? 0;
      const variantCount = variantFillCountByFamily[family] ?? 0;
      familyReviewRate[family] = familyCount === 0 ? 0 : reviewCount / familyCount;
      variantFillRateByFamily[family] = familyCount === 0 ? 0 : variantCount / familyCount;
    }
    const variantFillRate = processedCount === 0 ? 0 : variantFilledCount / processedCount;

    const preQaQualityGatePass =
      autoAcceptedRate >= 0.7 &&
      fallbackCategoryRate <= 0.05 &&
      attributeValidationFailRate <= 0.08 &&
      needsReviewRate <= 0.3;

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
      confusionHotlistFileName,
      confusionHotlistCsvContent: confusionHotlist.csvContent,
      categoryCount: drafts.length,
      needsReviewCount,
      autoAcceptedCount,
      autoAcceptedRate,
      fallbackCategoryCount,
      fallbackCategoryRate,
      categoryContradictionCount,
      attributeValidationFailCount,
      categoryConfidenceHistogram: categoryAssignments.confidenceHistogram,
      topConfusionAlerts: categoryAssignments.topConfusionAlerts,
      familyDistribution,
      familyReviewRate,
      variantFillRate,
      variantFillRateByFamily,
      taxonomyVersion: taxonomy.taxonomyVersion,
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
      auto_accepted_count: autoAcceptedCount,
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
        needs_review_rate: needsReviewRate,
        auto_accepted_count: autoAcceptedCount,
        auto_accepted_rate: autoAcceptedRate,
        fallback_category_count: fallbackCategoryCount,
        fallback_category_rate: fallbackCategoryRate,
        category_contradiction_count: categoryContradictionCount,
        attribute_validation_fail_count: attributeValidationFailCount,
        category_confidence_histogram: categoryAssignments.confidenceHistogram,
        top_confusion_alerts: categoryAssignments.topConfusionAlerts,
        family_distribution: familyDistribution,
        family_review_rate: familyReviewRate,
        variant_fill_rate: variantFillRate,
        variant_fill_rate_by_family: variantFillRateByFamily,
        taxonomy_version: taxonomy.taxonomyVersion,
        qa_sampled_rows: qaResult.sampledRows,
        qa_total_rows: qaResult.totalRows,
        qa_report_path: qaResult.filePath,
        confusion_hotlist_path: confusionHotlistPath,
        confusion_hotlist_count: confusionHotlist.rows.length,
        confusion_hotlist_top20: confusionHotlist.rows,
        quality_qa_sample_size: config.QUALITY_QA_SAMPLE_SIZE,
        quality_gate: {
          auto_accepted_rate_target: 0.7,
          fallback_category_rate_target: 0.05,
          attribute_validation_fail_rate_target: 0.08,
          needs_review_rate_target: 0.3,
          manual_qa_pass_rate_target: 0.9,
          critical_mismatch_rate_target: 0.02,
          pre_qa_passed: preQaQualityGatePass,
          manual_qa_pending: true,
          status: preQaQualityGatePass ? "pending_manual_qa" : "failed_pre_qa",
        },
        artifact_retention_hours: config.ARTIFACT_RETENTION_HOURS,
        artifacts: artifactBuild.artifactSummaries,
        artifact_cleanup_deleted_count: cleanedArtifacts,
        trace_cleanup_deleted_count: cleanedLogs,
        trace_retention_hours: config.TRACE_RETENTION_HOURS,
        trace_event_count: traceStats.trace_event_count,
        trace_openai_event_count: traceStats.trace_openai_event_count,
        trace_flush_error_count: traceStats.trace_flush_error_count,
        stale_runs_recovered_at_start: staleRunsRecovered,
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
        stale_runs_recovered_at_start: staleRunsRecovered,
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

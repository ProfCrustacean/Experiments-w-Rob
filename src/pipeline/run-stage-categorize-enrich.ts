import pLimit from "p-limit";
import type { AppConfig } from "../config.js";
import { RunLogger } from "../logging/run-logger.js";
import type {
  AttributeExtractionLLMOutput,
  EmbeddingProvider,
  LLMProvider,
  NormalizedCatalogProduct,
  PersistedCategory,
  ProductEnrichment,
} from "../types.js";
import { loadTaxonomy } from "../taxonomy/load.js";
import {
  assignCategoriesForProducts,
  type CategoryAssignmentOutput,
} from "./category-assignment.js";
import { enrichProductWithSignals } from "./enrichment.js";
import { upsertCategoryDrafts } from "./persist.js";
import {
  buildEscalationAttributeSchema,
  buildMissingCategoryEnrichment,
  isEnrichmentImproved,
  shouldLogProgress,
  type AttributeBatchItem,
  type AttributeBatchTask,
  type CategoryContext,
} from "./run-support.js";
import { buildCategoryDraftsFromTaxonomyAssignments } from "./taxonomy-category-drafts.js";

export interface EnrichmentStageStats {
  attributeBatchCount: number;
  attributeBatchFailureCount: number;
  attributeBatchFallbackProducts: number;
  attributeSecondPassCandidateProducts: number;
  attributeSecondPassBatchCount: number;
  attributeSecondPassFailureCount: number;
  attributeSecondPassFallbackProducts: number;
  attributeSecondPassAppliedProducts: number;
}

export interface CategorizeAndEnrichStageResult {
  categoryAssignments: CategoryAssignmentOutput;
  categoriesBySlug: Map<string, PersistedCategory>;
  categoryCount: number;
  taxonomyVersion: string;
  enrichmentMap: Map<string, ProductEnrichment>;
  enrichmentStats: EnrichmentStageStats;
}

export async function runCategorizeAndEnrichStage(input: {
  storeId: string;
  products: NormalizedCatalogProduct[];
  embeddingProvider: EmbeddingProvider;
  llmProvider: LLMProvider;
  usingOpenAI: boolean;
  config: AppConfig;
  logger: RunLogger;
  stageTimingsMs: Record<string, number>;
}): Promise<CategorizeAndEnrichStageResult> {
  input.logger.info("pipeline", "stage.started", "Starting categorization stage.", {
    stage_name: "categorization",
    product_count: input.products.length,
  });
  const categorizationStart = Date.now();
  const categoryAssignments = await assignCategoriesForProducts({
    products: input.products,
    embeddingProvider: input.embeddingProvider,
    llmProvider: input.llmProvider,
    autoMinConfidence: input.config.CATEGORY_AUTO_MIN_CONFIDENCE,
    autoMinMargin: input.config.CATEGORY_AUTO_MIN_MARGIN,
    highRiskExtraConfidence: input.config.HIGH_RISK_CATEGORY_EXTRA_CONFIDENCE,
    llmConcurrency: input.config.CATEGORY_PROFILE_CONCURRENCY,
    embeddingBatchSize: input.config.EMBEDDING_BATCH_SIZE,
    embeddingConcurrency: input.config.EMBEDDING_CONCURRENCY,
  });
  const taxonomy = loadTaxonomy();
  const assignedCategoryBySku = new Map<string, string>();
  for (const [sourceSku, assignment] of categoryAssignments.assignmentsBySku.entries()) {
    assignedCategoryBySku.set(sourceSku, assignment.categorySlug);
  }
  const assignedCategoryCount = new Set(assignedCategoryBySku.values()).size;
  input.stageTimingsMs.categorization_ms = Date.now() - categorizationStart;
  input.logger.info("pipeline", "stage.completed", "Categorization stage completed.", {
    stage_name: "categorization",
    elapsed_ms: input.stageTimingsMs.categorization_ms,
    category_count: assignedCategoryCount,
    confidence_histogram: categoryAssignments.confidenceHistogram,
  });

  input.logger.info("pipeline", "stage.started", "Starting category generation stage.", {
    stage_name: "category_generation",
    category_count: assignedCategoryCount,
  });
  const categoryGenerationStart = Date.now();
  const drafts = buildCategoryDraftsFromTaxonomyAssignments({
    assignedCategoryBySku,
  });
  input.stageTimingsMs.category_generation_ms = Date.now() - categoryGenerationStart;
  input.logger.info("pipeline", "stage.completed", "Category generation stage completed.", {
    stage_name: "category_generation",
    elapsed_ms: input.stageTimingsMs.category_generation_ms,
    category_count: drafts.length,
  });

  input.logger.info("pipeline", "stage.started", "Starting category persistence stage.", {
    stage_name: "category_persist",
    category_count: drafts.length,
  });
  const categoryUpsertStart = Date.now();
  const categoriesBySlug = await upsertCategoryDrafts(input.storeId, drafts);
  input.stageTimingsMs.category_upsert_ms = Date.now() - categoryUpsertStart;
  input.logger.info("pipeline", "stage.completed", "Category persistence stage completed.", {
    stage_name: "category_persist",
    elapsed_ms: input.stageTimingsMs.category_upsert_ms,
  });

  input.logger.info("pipeline", "stage.started", "Starting enrichment stage.", {
    stage_name: "enrichment",
  });
  const enrichmentStart = Date.now();
  const enrichmentMap = new Map<string, ProductEnrichment>();

  const batchedByCategory = new Map<string, AttributeBatchTask>();

  for (const product of input.products) {
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
      input.config.CONFIDENCE_THRESHOLD,
      input.config.ATTRIBUTE_AUTO_MIN_CONFIDENCE,
    );

    if (!input.usingOpenAI) {
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
        return attribute.required && confidence < input.config.ATTRIBUTE_AUTO_MIN_CONFIDENCE;
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
        llmAttributeSchema,
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
            llmAttributeSchema,
          },
        ],
      });
    }
  }

  let attributeBatchFallbackProducts = 0;
  let attributeBatchFailureCount = 0;
  let attributeBatchCount = 0;
  let attributeSecondPassCandidateProducts = 0;
  let attributeSecondPassBatchCount = 0;
  let attributeSecondPassFailureCount = 0;
  let attributeSecondPassFallbackProducts = 0;
  let attributeSecondPassAppliedProducts = 0;

  if (input.usingOpenAI) {
    const batchTasks: AttributeBatchTask[] = [];
    const firstPassItems: AttributeBatchItem[] = [];
    for (const task of batchedByCategory.values()) {
      for (let index = 0; index < task.items.length; index += input.config.ATTRIBUTE_BATCH_SIZE) {
        const taskItems = task.items.slice(index, index + input.config.ATTRIBUTE_BATCH_SIZE);
        batchTasks.push({
          categorySlug: task.categorySlug,
          categoryContext: task.categoryContext,
          llmAttributeSchema: task.llmAttributeSchema,
          items: taskItems,
        });
        firstPassItems.push(...taskItems);
      }
    }

    attributeBatchCount = batchTasks.length;
    let completedBatches = 0;
    const limiter = pLimit(input.config.ATTRIBUTE_LLM_CONCURRENCY);

    // eslint-disable-next-line no-console
    console.log(
      `[attribute_batches] ${batchTasks.length} batch requests (batch_size=${input.config.ATTRIBUTE_BATCH_SIZE}, concurrency=${input.config.ATTRIBUTE_LLM_CONCURRENCY})`,
    );

    await Promise.all(
      batchTasks.map((task) =>
        limiter(async () => {
          input.logger.debug("pipeline", "batch.attribute.started", "Attribute batch started.", {
            category_slug: task.categorySlug,
            sku_count: task.items.length,
            attribute_keys: task.llmAttributeSchema.attributes.map((attribute) => attribute.key),
            source_skus: task.items.map((item) => item.product.sourceSku),
          });

          let outputBySku: Record<string, AttributeExtractionLLMOutput> | null = null;

          try {
            outputBySku = await input.llmProvider.extractProductAttributesBatch({
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
            input.logger.warn("pipeline", "batch.attribute.failed", "Attribute batch failed.", {
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
              input.config.CONFIDENCE_THRESHOLD,
              input.config.ATTRIBUTE_AUTO_MIN_CONFIDENCE,
              fallbackReason ? { fallbackReason } : undefined,
            );

            enrichmentMap.set(product.sourceSku, enrichment);
          }

          completedBatches += 1;
          input.logger.info("pipeline", "batch.attribute.completed", "Attribute batch completed.", {
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

    if (input.config.ATTRIBUTE_SECOND_PASS_ENABLED && input.config.ATTRIBUTE_SECOND_PASS_MAX_PRODUCTS > 0) {
      const secondPassStart = Date.now();
      const secondPassCandidates = firstPassItems
        .map((item) => {
          const currentEnrichment = enrichmentMap.get(item.product.sourceSku);
          if (!currentEnrichment || item.context.isFallbackCategory) {
            return null;
          }

          const escalationSchema = buildEscalationAttributeSchema({
            context: item.context,
            enrichment: currentEnrichment,
            requiredMinConfidence: input.config.ATTRIBUTE_SECOND_PASS_REQUIRED_MIN_CONFIDENCE,
            optionalMinConfidence: input.config.ATTRIBUTE_SECOND_PASS_OPTIONAL_MIN_CONFIDENCE,
          });

          if (!escalationSchema) {
            return null;
          }

          return {
            product: item.product,
            context: item.context,
            llmAttributeSchema: escalationSchema,
            priority: currentEnrichment.uncertaintyReasons.length,
          };
        })
        .filter(
          (
            item,
          ): item is {
            product: NormalizedCatalogProduct;
            context: CategoryContext;
            llmAttributeSchema: CategoryContext["attributes"];
            priority: number;
          } => item !== null,
        )
        .sort((left, right) => {
          if (right.priority !== left.priority) {
            return right.priority - left.priority;
          }
          return left.product.sourceSku.localeCompare(right.product.sourceSku);
        })
        .slice(0, input.config.ATTRIBUTE_SECOND_PASS_MAX_PRODUCTS);

      attributeSecondPassCandidateProducts = secondPassCandidates.length;

      const secondPassGroupedTasks = new Map<string, AttributeBatchTask>();
      for (const candidate of secondPassCandidates) {
        const keys = candidate.llmAttributeSchema.attributes
          .map((attribute) => attribute.key)
          .sort();
        const taskKey = `${candidate.context.slug}::${keys.join(",")}`;
        const existingTask = secondPassGroupedTasks.get(taskKey);
        if (existingTask) {
          existingTask.items.push({
            product: candidate.product,
            context: candidate.context,
            llmAttributeSchema: candidate.llmAttributeSchema,
          });
          continue;
        }

        secondPassGroupedTasks.set(taskKey, {
          categorySlug: candidate.context.slug,
          categoryContext: {
            slug: candidate.context.slug,
            description: candidate.context.description,
          },
          llmAttributeSchema: candidate.llmAttributeSchema,
          items: [
            {
              product: candidate.product,
              context: candidate.context,
              llmAttributeSchema: candidate.llmAttributeSchema,
            },
          ],
        });
      }

      const secondPassTasks: AttributeBatchTask[] = [];
      for (const task of secondPassGroupedTasks.values()) {
        for (let index = 0; index < task.items.length; index += input.config.ATTRIBUTE_SECOND_PASS_BATCH_SIZE) {
          secondPassTasks.push({
            categorySlug: task.categorySlug,
            categoryContext: task.categoryContext,
            llmAttributeSchema: task.llmAttributeSchema,
            items: task.items.slice(index, index + input.config.ATTRIBUTE_SECOND_PASS_BATCH_SIZE),
          });
        }
      }

      attributeSecondPassBatchCount = secondPassTasks.length;
      if (secondPassTasks.length > 0) {
        // eslint-disable-next-line no-console
        console.log(
          `[attribute_second_pass] ${secondPassTasks.length} batch requests (batch_size=${input.config.ATTRIBUTE_SECOND_PASS_BATCH_SIZE}, model=${input.config.ATTRIBUTE_SECOND_PASS_MODEL})`,
        );
      }

      let completedSecondPassBatches = 0;
      const secondPassLimiter = pLimit(input.config.ATTRIBUTE_LLM_CONCURRENCY);
      await Promise.all(
        secondPassTasks.map((task) =>
          secondPassLimiter(async () => {
            input.logger.debug("pipeline", "batch.attribute.second_pass.started", "Attribute second-pass batch started.", {
              category_slug: task.categorySlug,
              sku_count: task.items.length,
              attribute_keys: task.llmAttributeSchema.attributes.map((attribute) => attribute.key),
              source_skus: task.items.map((item) => item.product.sourceSku),
              escalation_model: input.config.ATTRIBUTE_SECOND_PASS_MODEL,
            });

            let outputBySku: Record<string, AttributeExtractionLLMOutput> | null = null;
            try {
              outputBySku = await input.llmProvider.extractProductAttributesBatch({
                categoryName: task.llmAttributeSchema.category_name_pt,
                categoryDescription: task.categoryContext.description,
                attributeSchema: task.llmAttributeSchema,
                model: input.config.ATTRIBUTE_SECOND_PASS_MODEL,
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
              attributeSecondPassFailureCount += 1;
              outputBySku = null;
              input.logger.warn(
                "pipeline",
                "batch.attribute.second_pass.failed",
                "Attribute second-pass batch failed.",
                {
                  category_slug: task.categorySlug,
                  sku_count: task.items.length,
                  error_message: error instanceof Error ? error.message : "unknown_error",
                  escalation_model: input.config.ATTRIBUTE_SECOND_PASS_MODEL,
                },
              );
            }

            for (const item of task.items) {
              const currentEnrichment = enrichmentMap.get(item.product.sourceSku);
              if (!currentEnrichment) {
                continue;
              }

              const llmOutput = outputBySku?.[item.product.sourceSku] ?? null;
              if (!llmOutput) {
                attributeSecondPassFallbackProducts += 1;
                continue;
              }

              const candidateEnrichment = enrichProductWithSignals(
                item.product,
                item.context,
                llmOutput,
                input.config.CONFIDENCE_THRESHOLD,
                input.config.ATTRIBUTE_AUTO_MIN_CONFIDENCE,
              );

              if (isEnrichmentImproved(currentEnrichment, candidateEnrichment)) {
                enrichmentMap.set(item.product.sourceSku, candidateEnrichment);
                attributeSecondPassAppliedProducts += 1;
              }
            }

            completedSecondPassBatches += 1;
            input.logger.info(
              "pipeline",
              "batch.attribute.second_pass.completed",
              "Attribute second-pass batch completed.",
              {
                category_slug: task.categorySlug,
                completed_batches: completedSecondPassBatches,
                total_batches: secondPassTasks.length,
                escalation_model: input.config.ATTRIBUTE_SECOND_PASS_MODEL,
              },
            );
            if (shouldLogProgress(completedSecondPassBatches, secondPassTasks.length)) {
              // eslint-disable-next-line no-console
              console.log(
                `[attribute_second_pass] ${completedSecondPassBatches}/${secondPassTasks.length} completed`,
              );
            }
          }),
        ),
      );
      input.stageTimingsMs.attribute_second_pass_ms = Date.now() - secondPassStart;
    }
  }

  for (const product of input.products) {
    if (!enrichmentMap.has(product.sourceSku)) {
      enrichmentMap.set(product.sourceSku, buildMissingCategoryEnrichment(product.sourceSku));
    }
  }

  input.stageTimingsMs.enrichment_ms = Date.now() - enrichmentStart;
  input.logger.info("pipeline", "stage.completed", "Enrichment stage completed.", {
    stage_name: "enrichment",
    elapsed_ms: input.stageTimingsMs.enrichment_ms,
    attribute_batch_count: attributeBatchCount,
    attribute_batch_failure_count: attributeBatchFailureCount,
    attribute_batch_fallback_products: attributeBatchFallbackProducts,
    attribute_second_pass_candidate_products: attributeSecondPassCandidateProducts,
    attribute_second_pass_batch_count: attributeSecondPassBatchCount,
    attribute_second_pass_failure_count: attributeSecondPassFailureCount,
    attribute_second_pass_fallback_products: attributeSecondPassFallbackProducts,
    attribute_second_pass_applied_products: attributeSecondPassAppliedProducts,
  });

  return {
    categoryAssignments,
    categoriesBySlug,
    categoryCount: drafts.length,
    taxonomyVersion: taxonomy.taxonomyVersion,
    enrichmentMap,
    enrichmentStats: {
      attributeBatchCount,
      attributeBatchFailureCount,
      attributeBatchFallbackProducts,
      attributeSecondPassCandidateProducts,
      attributeSecondPassBatchCount,
      attributeSecondPassFailureCount,
      attributeSecondPassFallbackProducts,
      attributeSecondPassAppliedProducts,
    },
  };
}

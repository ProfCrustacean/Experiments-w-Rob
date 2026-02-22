import path from "node:path";
import { getConfig } from "../config.js";
import type { PipelineRunSummary } from "../types.js";
import { runMigrations } from "../db/migrate.js";
import {
  createPipelineRun,
  insertRunLogBatch,
  recoverStaleRunningRuns,
} from "./persist.js";
import { RunLogger } from "../logging/run-logger.js";
import { runIngestAndSampling } from "./run-stage-ingest.js";
import { runEmbeddingStage } from "./run-stage-embedding.js";
import { runProductPersistenceStage } from "./run-stage-product-persist.js";
import { runReportingStage } from "./run-stage-reporting.js";
import { computeQualityMetrics } from "./run-stage-metrics.js";
import { runArtifactPersistAndCleanupStages } from "./run-stage-artifact-persist.js";
import { runStartupStage } from "./run-stage-startup.js";
import { runArtifactGenerationStage } from "./run-stage-artifact-generation.js";
import { finalizeCompletedRun, finalizeFailedRun } from "./run-stage-finalize.js";
import { runCategorizeAndEnrichStage } from "./run-stage-categorize-enrich.js";
import { type RunPipelineInput, validateSamplingInput } from "./run-support.js";

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

  const ingestSampling = await runIngestAndSampling({
    inputPath: input.inputPath,
    storeId: input.storeId,
    sampleParts,
    samplePartIndex,
  });
  const {
    sourceRows,
    deduplicatedRows: deduplicated,
    normalizedRows: normalized,
    partitioned,
    ingestElapsedMs,
    samplingElapsedMs,
  } = ingestSampling;

  const stageStartAt = new Date();
  const stageTimingsMs: Record<string, number> = {};
  stageTimingsMs.ingest_ms = ingestElapsedMs;
  stageTimingsMs.sampling_ms = samplingElapsedMs;
  const inputFileName = path.basename(input.inputPath);

  const runId = await createPipelineRun({
    storeId: input.storeId,
    inputFileName,
    runLabel: input.runLabel,
  });

  const logger = new RunLogger({
    runId,
    traceRetentionHours: config.TRACE_RETENTION_HOURS,
    flushBatchSize: config.TRACE_FLUSH_BATCH_SIZE,
    insertBatch: insertRunLogBatch,
  });

  try {
    const { embeddingProvider, llmProvider, usingOpenAI, openAIProvider } = runStartupStage({
      runId,
      storeId: input.storeId,
      inputFileName,
      sampleParts,
      samplePartIndex,
      sourceRowCount: sourceRows.length,
      deduplicatedRowCount: deduplicated.length,
      normalizedRowCount: normalized.length,
      sampledRowCount: partitioned.sampled.length,
      skippedRowCount: partitioned.skipped,
      ingestElapsedMs,
      samplingElapsedMs,
      config,
      logger,
    });

    const categorizeEnrichStage = await runCategorizeAndEnrichStage({
      storeId: input.storeId,
      products: partitioned.sampled,
      embeddingProvider,
      llmProvider,
      usingOpenAI,
      config,
      logger,
      stageTimingsMs,
    });
    const {
      categoryAssignments,
      categoriesBySlug,
      categoryCount,
      taxonomyVersion,
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
    } = categorizeEnrichStage;

    const { vectorsBySku, embeddedTextBySku } = await runEmbeddingStage({
      products: partitioned.sampled,
      enrichments: enrichmentMap,
      categoriesBySlug,
      embeddingProvider,
      embeddingBatchSize: config.EMBEDDING_BATCH_SIZE,
      embeddingConcurrency: config.EMBEDDING_CONCURRENCY,
      logger,
      stageTimingsMs,
    });

    await runProductPersistenceStage({
      storeId: input.storeId,
      runId,
      products: partitioned.sampled,
      enrichments: enrichmentMap,
      categoriesBySlug,
      vectorsBySku,
      embeddedTextBySku,
      embeddingModel: config.EMBEDDING_MODEL,
      queryTimeoutMs: config.PRODUCT_VECTOR_QUERY_TIMEOUT_MS,
      vectorBatchSize: config.PRODUCT_VECTOR_BATCH_SIZE,
      persistStageTimeoutMs: config.PRODUCT_PERSIST_STAGE_TIMEOUT_MS,
      logger,
      stageTimingsMs,
    });
    const reportingStage = await runReportingStage({
      runId,
      outputDir: config.OUTPUT_DIR,
      qaSampleSize: config.QUALITY_QA_SAMPLE_SIZE,
      products: partitioned.sampled,
      enrichments: enrichmentMap,
      assignmentsBySku: categoryAssignments.assignmentsBySku,
      logger,
      stageTimingsMs,
    });
    const { qaRows, qaResult, confusionHotlist, confusionHotlistFileName, confusionHotlistPath } =
      reportingStage;

    const processedCount = partitioned.sampled.length;
    const qualityMetrics = computeQualityMetrics({
      qaRows,
      enrichments: enrichmentMap,
      processedCount,
    });
    const openAIStats = usingOpenAI ? openAIProvider?.getStats() ?? null : null;
    const artifactGeneration = runArtifactGenerationStage({
      runId,
      storeId: input.storeId,
      inputFileName,
      products: partitioned.sampled,
      enrichments: enrichmentMap,
      categoriesBySlug,
      qaReportFileName: qaResult.fileName,
      qaReportCsvContent: qaResult.csvContent,
      confusionHotlistFileName,
      confusionHotlistCsvContent: confusionHotlist.csvContent,
      categoryCount,
      needsReviewCount: qualityMetrics.needsReviewCount,
      autoAcceptedCount: qualityMetrics.autoAcceptedCount,
      autoAcceptedRate: qualityMetrics.autoAcceptedRate,
      fallbackCategoryCount: qualityMetrics.fallbackCategoryCount,
      fallbackCategoryRate: qualityMetrics.fallbackCategoryRate,
      categoryContradictionCount: qualityMetrics.categoryContradictionCount,
      attributeValidationFailCount: qualityMetrics.attributeValidationFailCount,
      categoryConfidenceHistogram: categoryAssignments.confidenceHistogram,
      topConfusionAlerts: categoryAssignments.topConfusionAlerts,
      familyDistribution: qualityMetrics.familyDistribution,
      familyReviewRate: qualityMetrics.familyReviewRate,
      variantFillRate: qualityMetrics.variantFillRate,
      variantFillRateByFamily: qualityMetrics.variantFillRateByFamily,
      taxonomyVersion,
      stageTimingsMs,
      openAIEnabled: usingOpenAI,
      openAIRequestStats: openAIStats,
      attributeBatchFailureCount,
      attributeBatchFallbackProducts,
      startedAt: stageStartAt,
      artifactRetentionHours: config.ARTIFACT_RETENTION_HOURS,
      logger,
    });

    const { cleanedArtifacts, cleanedLogs } = await runArtifactPersistAndCleanupStages({
      runId,
      outputDir: config.OUTPUT_DIR,
      artifacts: artifactGeneration.artifacts,
      artifactsExpireAt: artifactGeneration.artifactsExpireAt,
      artifactRetentionHours: config.ARTIFACT_RETENTION_HOURS,
      traceRetentionHours: config.TRACE_RETENTION_HOURS,
      logger,
      stageTimingsMs,
    });

    return await finalizeCompletedRun({
      runId,
      storeId: input.storeId,
      inputFileName,
      totalRows: sourceRows.length,
      uniqueProductsInput: normalized.length,
      uniqueProductsProcessed: partitioned.sampled.length,
      uniqueProductsSkipped: partitioned.skipped,
      sampleParts,
      samplePartIndex,
      categoryCount,
      taxonomyVersion,
      qualityMetrics,
      categoryAssignments,
      qaSampledRows: qaResult.sampledRows,
      qaTotalRows: qaResult.totalRows,
      qaReportPath: qaResult.filePath,
      confusionHotlistPath,
      confusionHotlistRows: confusionHotlist.rows,
      qualityQaSampleSize: config.QUALITY_QA_SAMPLE_SIZE,
      artifactRetentionHours: config.ARTIFACT_RETENTION_HOURS,
      artifactSummaries: artifactGeneration.artifactSummaries,
      cleanedArtifacts,
      cleanedLogs,
      traceRetentionHours: config.TRACE_RETENTION_HOURS,
      staleRunsRecoveredAtStart: staleRunsRecovered,
      openAIEnabled: usingOpenAI,
      openAIRequestStats: openAIStats,
      attributeStats: {
        attributeBatchCount,
        attributeBatchFailureCount,
        attributeBatchFallbackProducts,
        attributeSecondPassCandidateProducts,
        attributeSecondPassBatchCount,
        attributeSecondPassFailureCount,
        attributeSecondPassFallbackProducts,
        attributeSecondPassAppliedProducts,
      },
      attributeSecondPassModel: config.ATTRIBUTE_SECOND_PASS_MODEL,
      stageTimingsMs,
      startedAt: stageStartAt,
      logger,
    });
  } catch (error) {
    await finalizeFailedRun({
      runId,
      error,
      stageTimingsMs,
      sampleParts,
      samplePartIndex,
      traceRetentionHours: config.TRACE_RETENTION_HOURS,
      staleRunsRecoveredAtStart: staleRunsRecovered,
      logger,
    });

    throw error;
  }
}

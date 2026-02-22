import { RunLogger } from "../logging/run-logger.js";
import type { PipelineRunSummary, RunArtifactSummary } from "../types.js";
import type { CategoryAssignmentOutput } from "./category-assignment.js";
import type { ConfusionHotlistRow } from "./confusion-hotlist.js";
import { finalizePipelineRun } from "./persist.js";
import type { QualityMetricsResult } from "./run-stage-metrics.js";

export interface CompletedRunAttributeStats {
  attributeBatchCount: number;
  attributeBatchFailureCount: number;
  attributeBatchFallbackProducts: number;
  attributeSecondPassCandidateProducts: number;
  attributeSecondPassBatchCount: number;
  attributeSecondPassFailureCount: number;
  attributeSecondPassFallbackProducts: number;
  attributeSecondPassAppliedProducts: number;
}

export async function finalizeCompletedRun(input: {
  runId: string;
  storeId: string;
  inputFileName: string;
  totalRows: number;
  uniqueProductsInput: number;
  uniqueProductsProcessed: number;
  uniqueProductsSkipped: number;
  sampleParts: number;
  samplePartIndex: number;
  categoryCount: number;
  taxonomyVersion: string;
  qualityMetrics: QualityMetricsResult;
  categoryAssignments: Pick<CategoryAssignmentOutput, "confidenceHistogram" | "topConfusionAlerts">;
  qaSampledRows: number;
  qaTotalRows: number;
  qaReportPath: string;
  confusionHotlistPath: string;
  confusionHotlistRows: ConfusionHotlistRow[];
  qualityQaSampleSize: number;
  artifactRetentionHours: number;
  artifactSummaries: RunArtifactSummary[];
  cleanedArtifacts: number;
  cleanedLogs: number;
  traceRetentionHours: number;
  staleRunsRecoveredAtStart: number;
  openAIEnabled: boolean;
  openAIRequestStats: unknown;
  attributeStats: CompletedRunAttributeStats;
  attributeSecondPassModel: string;
  stageTimingsMs: Record<string, number>;
  startedAt: Date;
  logger: RunLogger;
}): Promise<PipelineRunSummary> {
  input.logger.info("pipeline", "run.completed", "Pipeline run completed successfully.", {
    run_id: input.runId,
    processed_products: input.uniqueProductsProcessed,
    category_count: input.categoryCount,
    needs_review_count: input.qualityMetrics.needsReviewCount,
    auto_accepted_count: input.qualityMetrics.autoAcceptedCount,
  });

  await input.logger.flush("run_completed");
  const traceStats = input.logger.getStats();

  await finalizePipelineRun({
    runId: input.runId,
    status: "completed_pending_review",
    stats: {
      total_rows: input.totalRows,
      unique_products_input: input.uniqueProductsInput,
      unique_products_processed: input.uniqueProductsProcessed,
      unique_products_skipped: input.uniqueProductsSkipped,
      sample_parts: input.sampleParts,
      sample_part_index: input.samplePartIndex,
      category_count: input.categoryCount,
      needs_review_count: input.qualityMetrics.needsReviewCount,
      needs_review_rate: input.qualityMetrics.needsReviewRate,
      auto_accepted_count: input.qualityMetrics.autoAcceptedCount,
      auto_accepted_rate: input.qualityMetrics.autoAcceptedRate,
      fallback_category_count: input.qualityMetrics.fallbackCategoryCount,
      fallback_category_rate: input.qualityMetrics.fallbackCategoryRate,
      category_contradiction_count: input.qualityMetrics.categoryContradictionCount,
      attribute_validation_fail_count: input.qualityMetrics.attributeValidationFailCount,
      category_confidence_histogram: input.categoryAssignments.confidenceHistogram,
      top_confusion_alerts: input.categoryAssignments.topConfusionAlerts,
      family_distribution: input.qualityMetrics.familyDistribution,
      family_review_rate: input.qualityMetrics.familyReviewRate,
      variant_fill_rate: input.qualityMetrics.variantFillRate,
      variant_fill_rate_by_family: input.qualityMetrics.variantFillRateByFamily,
      taxonomy_version: input.taxonomyVersion,
      qa_sampled_rows: input.qaSampledRows,
      qa_total_rows: input.qaTotalRows,
      qa_report_path: input.qaReportPath,
      confusion_hotlist_path: input.confusionHotlistPath,
      confusion_hotlist_count: input.confusionHotlistRows.length,
      confusion_hotlist_top20: input.confusionHotlistRows,
      quality_qa_sample_size: input.qualityQaSampleSize,
      quality_gate: {
        auto_accepted_rate_target: 0.7,
        fallback_category_rate_target: 0.05,
        attribute_validation_fail_rate_target: 0.08,
        needs_review_rate_target: 0.3,
        manual_qa_pass_rate_target: 0.9,
        critical_mismatch_rate_target: 0.02,
        pre_qa_passed: input.qualityMetrics.preQaQualityGatePass,
        manual_qa_pending: true,
        status: input.qualityMetrics.preQaQualityGatePass
          ? "pending_manual_qa"
          : "failed_pre_qa",
      },
      artifact_retention_hours: input.artifactRetentionHours,
      artifacts: input.artifactSummaries,
      artifact_cleanup_deleted_count: input.cleanedArtifacts,
      trace_cleanup_deleted_count: input.cleanedLogs,
      trace_retention_hours: input.traceRetentionHours,
      trace_event_count: traceStats.trace_event_count,
      trace_openai_event_count: traceStats.trace_openai_event_count,
      trace_flush_error_count: traceStats.trace_flush_error_count,
      stale_runs_recovered_at_start: input.staleRunsRecoveredAtStart,
      openai_enabled: input.openAIEnabled,
      attribute_batch_count: input.attributeStats.attributeBatchCount,
      attribute_batch_failure_count: input.attributeStats.attributeBatchFailureCount,
      attribute_batch_fallback_products: input.attributeStats.attributeBatchFallbackProducts,
      attribute_second_pass_candidate_products:
        input.attributeStats.attributeSecondPassCandidateProducts,
      attribute_second_pass_batch_count: input.attributeStats.attributeSecondPassBatchCount,
      attribute_second_pass_failure_count: input.attributeStats.attributeSecondPassFailureCount,
      attribute_second_pass_fallback_products:
        input.attributeStats.attributeSecondPassFallbackProducts,
      attribute_second_pass_applied_products: input.attributeStats.attributeSecondPassAppliedProducts,
      attribute_second_pass_model: input.attributeSecondPassModel,
      stage_timings_ms: input.stageTimingsMs,
      openai_request_stats: input.openAIRequestStats,
    },
  });

  // eslint-disable-next-line no-console
  console.log(
    `[run_summary] completed run ${input.runId} with ${input.uniqueProductsProcessed} products in ${Math.round((Date.now() - input.startedAt.getTime()) / 1000)}s`,
  );

  return {
    runId: input.runId,
    storeId: input.storeId,
    inputFileName: input.inputFileName,
    totalRows: input.totalRows,
    uniqueProducts: input.uniqueProductsProcessed,
    categoryCount: input.categoryCount,
    needsReviewCount: input.qualityMetrics.needsReviewCount,
    qaReportPath: input.qaReportPath,
    artifacts: input.artifactSummaries,
    status: "completed_pending_review",
  };
}

export async function finalizeFailedRun(input: {
  runId: string;
  error: unknown;
  stageTimingsMs: Record<string, number>;
  sampleParts: number;
  samplePartIndex: number;
  traceRetentionHours: number;
  staleRunsRecoveredAtStart: number;
  logger: RunLogger;
}): Promise<void> {
  const errorMessage = input.error instanceof Error ? input.error.message : "unknown_error";

  input.logger.error("pipeline", "run.failed", "Pipeline run failed.", {
    run_id: input.runId,
    error_message: errorMessage,
  });

  await input.logger.flush("run_failed");
  const traceStats = input.logger.getStats();

  await finalizePipelineRun({
    runId: input.runId,
    status: "failed",
    stats: {
      error_message: errorMessage,
      stage_timings_ms: input.stageTimingsMs,
      sample_parts: input.sampleParts,
      sample_part_index: input.samplePartIndex,
      trace_retention_hours: input.traceRetentionHours,
      trace_event_count: traceStats.trace_event_count,
      trace_openai_event_count: traceStats.trace_openai_event_count,
      trace_flush_error_count: traceStats.trace_flush_error_count,
      stale_runs_recovered_at_start: input.staleRunsRecoveredAtStart,
    },
  });
}

import { RunLogger } from "../logging/run-logger.js";
import type {
  NormalizedCatalogProduct,
  PersistedCategory,
  ProductEnrichment,
  RunArtifactSummary,
} from "../types.js";
import type { RunArtifactPayload } from "./run-artifacts.js";
import { buildRunArtifacts } from "./run-artifacts.js";

interface TopConfusionAlert {
  category_slug: string;
  affected_count: number;
  low_margin_count: number;
  contradiction_count: number;
  fallback_count: number;
}

export interface ArtifactGenerationResult {
  artifacts: RunArtifactPayload[];
  artifactSummaries: RunArtifactSummary[];
  artifactsExpireAt: Date;
}

export function runArtifactGenerationStage(input: {
  runId: string;
  storeId: string;
  inputFileName: string;
  products: NormalizedCatalogProduct[];
  enrichments: Map<string, ProductEnrichment>;
  categoriesBySlug: Map<string, PersistedCategory>;
  qaReportFileName: string;
  qaReportCsvContent: string;
  confusionHotlistFileName: string;
  confusionHotlistCsvContent: string;
  categoryCount: number;
  needsReviewCount: number;
  autoAcceptedCount: number;
  autoAcceptedRate: number;
  fallbackCategoryCount: number;
  fallbackCategoryRate: number;
  categoryContradictionCount: number;
  attributeValidationFailCount: number;
  categoryConfidenceHistogram: Record<string, number>;
  topConfusionAlerts: TopConfusionAlert[];
  familyDistribution: Record<string, number>;
  familyReviewRate: Record<string, number>;
  variantFillRate: number;
  variantFillRateByFamily: Record<string, number>;
  taxonomyVersion: string;
  stageTimingsMs: Record<string, number>;
  openAIEnabled: boolean;
  openAIRequestStats: unknown;
  attributeBatchFailureCount: number;
  attributeBatchFallbackProducts: number;
  startedAt: Date;
  artifactRetentionHours: number;
  logger: RunLogger;
}): ArtifactGenerationResult {
  const runFinishedAt = new Date();
  const artifactsExpireAt = new Date(
    runFinishedAt.getTime() + input.artifactRetentionHours * 60 * 60 * 1000,
  );

  input.logger.info("pipeline", "stage.started", "Starting artifact generation stage.", {
    stage_name: "artifact_generation",
  });
  const artifactBuildStart = Date.now();
  const artifactBuild = buildRunArtifacts({
    runId: input.runId,
    storeId: input.storeId,
    inputFileName: input.inputFileName,
    products: input.products,
    enrichments: input.enrichments,
    categoriesBySlug: input.categoriesBySlug,
    qaReportFileName: input.qaReportFileName,
    qaReportCsvContent: input.qaReportCsvContent,
    confusionHotlistFileName: input.confusionHotlistFileName,
    confusionHotlistCsvContent: input.confusionHotlistCsvContent,
    categoryCount: input.categoryCount,
    needsReviewCount: input.needsReviewCount,
    autoAcceptedCount: input.autoAcceptedCount,
    autoAcceptedRate: input.autoAcceptedRate,
    fallbackCategoryCount: input.fallbackCategoryCount,
    fallbackCategoryRate: input.fallbackCategoryRate,
    categoryContradictionCount: input.categoryContradictionCount,
    attributeValidationFailCount: input.attributeValidationFailCount,
    categoryConfidenceHistogram: input.categoryConfidenceHistogram,
    topConfusionAlerts: input.topConfusionAlerts,
    familyDistribution: input.familyDistribution,
    familyReviewRate: input.familyReviewRate,
    variantFillRate: input.variantFillRate,
    variantFillRateByFamily: input.variantFillRateByFamily,
    taxonomyVersion: input.taxonomyVersion,
    stageTimingsMs: input.stageTimingsMs,
    openAIEnabled: input.openAIEnabled,
    openAIRequestStats: input.openAIRequestStats,
    attributeBatchFailureCount: input.attributeBatchFailureCount,
    attributeBatchFallbackProducts: input.attributeBatchFallbackProducts,
    startedAt: input.startedAt,
    finishedAt: runFinishedAt,
    expiresAt: artifactsExpireAt,
  });
  input.stageTimingsMs.artifact_generation_ms = Date.now() - artifactBuildStart;
  input.logger.info("pipeline", "stage.completed", "Artifact generation stage completed.", {
    stage_name: "artifact_generation",
    elapsed_ms: input.stageTimingsMs.artifact_generation_ms,
    artifact_count: artifactBuild.artifacts.length,
  });

  return {
    artifacts: artifactBuild.artifacts,
    artifactSummaries: artifactBuild.artifactSummaries,
    artifactsExpireAt,
  };
}

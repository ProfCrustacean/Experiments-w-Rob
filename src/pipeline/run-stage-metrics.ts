import type { ProductEnrichment } from "../types.js";
import { hasVariantValues } from "./run-support.js";
import type { PipelineQaRow } from "./run-stage-reporting.js";

export interface QualityMetricsResult {
  needsReviewCount: number;
  autoAcceptedCount: number;
  fallbackCategoryCount: number;
  categoryContradictionCount: number;
  attributeValidationFailCount: number;
  autoAcceptedRate: number;
  fallbackCategoryRate: number;
  needsReviewRate: number;
  attributeValidationFailRate: number;
  familyDistribution: Record<string, number>;
  familyReviewRate: Record<string, number>;
  variantFillRate: number;
  variantFillRateByFamily: Record<string, number>;
  preQaQualityGatePass: boolean;
}

export function computeQualityMetrics(input: {
  qaRows: PipelineQaRow[];
  enrichments: Map<string, ProductEnrichment>;
  processedCount: number;
}): QualityMetricsResult {
  const needsReviewCount = input.qaRows.filter((row) => row.needsReview).length;
  const autoAcceptedCount = input.qaRows.filter((row) => row.autoDecision === "auto").length;
  const fallbackCategoryCount = [...input.enrichments.values()].filter(
    (enrichment) => enrichment.isFallbackCategory,
  ).length;
  const categoryContradictionCount = [...input.enrichments.values()].reduce(
    (sum, enrichment) => sum + enrichment.categoryContradictionCount,
    0,
  );
  const attributeValidationFailCount = [...input.enrichments.values()].reduce(
    (sum, enrichment) => sum + enrichment.attributeValidationFailCount,
    0,
  );

  const autoAcceptedRate =
    input.processedCount === 0 ? 0 : autoAcceptedCount / input.processedCount;
  const fallbackCategoryRate =
    input.processedCount === 0 ? 0 : fallbackCategoryCount / input.processedCount;
  const needsReviewRate = input.processedCount === 0 ? 0 : needsReviewCount / input.processedCount;
  const attributeValidationFailRate =
    input.processedCount === 0 ? 0 : attributeValidationFailCount / input.processedCount;

  const familyDistribution: Record<string, number> = {};
  const familyNeedsReviewCount: Record<string, number> = {};
  const variantFillCountByFamily: Record<string, number> = {};
  let variantFilledCount = 0;

  for (const enrichment of input.enrichments.values()) {
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
  const variantFillRate = input.processedCount === 0 ? 0 : variantFilledCount / input.processedCount;

  const preQaQualityGatePass =
    autoAcceptedRate >= 0.7 &&
    fallbackCategoryRate <= 0.05 &&
    attributeValidationFailRate <= 0.08 &&
    needsReviewRate <= 0.3;

  return {
    needsReviewCount,
    autoAcceptedCount,
    fallbackCategoryCount,
    categoryContradictionCount,
    attributeValidationFailCount,
    autoAcceptedRate,
    fallbackCategoryRate,
    needsReviewRate,
    attributeValidationFailRate,
    familyDistribution,
    familyReviewRate,
    variantFillRate,
    variantFillRateByFamily,
    preQaQualityGatePass,
  };
}

import type { SelfCorrectionContext } from "../types.js";

function asObject(value: unknown): Record<string, unknown> {
  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function asNumber(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function asAlerts(value: unknown): Array<{
  category_slug: string;
  affected_count: number;
  low_margin_count: number;
  contradiction_count: number;
  fallback_count: number;
}> {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((entry) => asObject(entry))
    .map((entry) => ({
      category_slug: String(entry.category_slug ?? ""),
      affected_count: Number(entry.affected_count ?? 0),
      low_margin_count: Number(entry.low_margin_count ?? 0),
      contradiction_count: Number(entry.contradiction_count ?? 0),
      fallback_count: Number(entry.fallback_count ?? 0),
    }))
    .filter((entry) => entry.category_slug.length > 0);
}

function extractFailedGateMetrics(stats: Record<string, unknown>): string[] {
  const qualityGate = asObject(stats.quality_gate);
  const failed: string[] = [];

  const autoAcceptedRate = asNumber(stats.auto_accepted_rate);
  const autoAcceptedTarget = asNumber(qualityGate.auto_accepted_rate_target);
  if (
    autoAcceptedRate !== null &&
    autoAcceptedTarget !== null &&
    autoAcceptedRate < autoAcceptedTarget
  ) {
    failed.push("auto_accepted_rate");
  }

  const fallbackRate = asNumber(stats.fallback_category_rate);
  const fallbackTarget = asNumber(qualityGate.fallback_category_rate_target);
  if (fallbackRate !== null && fallbackTarget !== null && fallbackRate > fallbackTarget) {
    failed.push("fallback_category_rate");
  }

  const attributeValidationFailRate = asNumber(stats.attribute_validation_fail_count);
  const attributeValidationTarget = asNumber(qualityGate.attribute_validation_fail_rate_target);
  const processed = Math.max(1, Number(stats.unique_products_processed ?? 0));
  const normalizedValidationRate =
    attributeValidationFailRate !== null ? attributeValidationFailRate / processed : null;
  if (
    normalizedValidationRate !== null &&
    attributeValidationTarget !== null &&
    normalizedValidationRate > attributeValidationTarget
  ) {
    failed.push("attribute_validation_fail_rate");
  }

  const needsReviewRate = asNumber(stats.needs_review_rate);
  const needsReviewTarget = asNumber(qualityGate.needs_review_rate_target);
  if (needsReviewRate !== null && needsReviewTarget !== null && needsReviewRate > needsReviewTarget) {
    failed.push("needs_review_rate");
  }

  if (qualityGate.pre_qa_passed === false && failed.length === 0) {
    failed.push("pre_qa_failed_unknown");
  }

  return [...new Set(failed)];
}

function buildCandidateFixes(input: {
  failedGateMetrics: string[];
  alerts: Array<{
    category_slug: string;
    affected_count: number;
    low_margin_count: number;
    contradiction_count: number;
    fallback_count: number;
  }>;
}): string[] {
  const fixes: string[] = [];

  for (const metric of input.failedGateMetrics) {
    if (metric === "auto_accepted_rate") {
      fixes.push("review_category_thresholds_for_low_auto_acceptance");
    } else if (metric === "fallback_category_rate") {
      fixes.push("tighten_fallback_rescue_rules_for_specific_families");
    } else if (metric === "attribute_validation_fail_rate") {
      fixes.push("tighten_attribute_policy_validation_or_schema_constraints");
    } else if (metric === "needs_review_rate") {
      fixes.push("improve_disambiguation_for_review_heavy_categories");
    } else {
      fixes.push(`investigate_gate_metric_${metric}`);
    }
  }

  const topAlert = [...input.alerts]
    .sort(
      (left, right) =>
        right.contradiction_count + right.low_margin_count - (left.contradiction_count + left.low_margin_count),
    )
    .slice(0, 3);

  for (const alert of topAlert) {
    fixes.push(`inspect_confusion_pair_${alert.category_slug}`);
  }

  return [...new Set(fixes)];
}

export function buildSelfCorrectionContext(input: {
  error: unknown;
  stats: Record<string, unknown>;
}): SelfCorrectionContext {
  const alerts = asAlerts(input.stats.top_confusion_alerts);
  const failedGateMetrics = extractFailedGateMetrics(input.stats);
  const candidateFixes = buildCandidateFixes({
    failedGateMetrics,
    alerts,
  });

  return {
    failureSummary: input.error instanceof Error ? input.error.message : String(input.error ?? "unknown_error"),
    lastConfusionAlerts: alerts,
    failedGateMetrics,
    candidateFixes,
  };
}

export function buildLearningCandidateFixesFromStats(
  stats: Record<string, unknown>,
): string[] {
  const alerts = asAlerts(stats.top_confusion_alerts);
  const failedGateMetrics = extractFailedGateMetrics(stats);
  return buildCandidateFixes({
    failedGateMetrics,
    alerts,
  });
}

export function evaluateQualityGateFromStats(input: {
  stats: Record<string, unknown>;
  canaryThreshold?: number;
  requireCanaryThreshold?: boolean;
}): {
  passed: boolean;
  basePassed: boolean;
  canaryThresholdPassed: boolean;
  failedMetrics: string[];
  baseFailedMetrics: string[];
  metrics: Record<string, unknown>;
} {
  const baseFailedMetrics = extractFailedGateMetrics(input.stats);
  const failedMetrics = [...baseFailedMetrics];
  let canaryThresholdPassed = true;
  const metrics: Record<string, unknown> = {
    auto_accepted_rate: input.stats.auto_accepted_rate ?? null,
    fallback_category_rate: input.stats.fallback_category_rate ?? null,
    needs_review_rate: input.stats.needs_review_rate ?? null,
    quality_gate: asObject(input.stats.quality_gate),
  };

  if (input.requireCanaryThreshold) {
    const threshold = input.canaryThreshold ?? 0;
    const autoAcceptedRate = asNumber(input.stats.auto_accepted_rate);
    metrics.canary_auto_accepted_threshold = threshold;
    metrics.canary_auto_accepted_rate = autoAcceptedRate;
    if (autoAcceptedRate === null || autoAcceptedRate < threshold) {
      failedMetrics.push("canary_auto_accepted_rate");
      canaryThresholdPassed = false;
    }
  }

  return {
    passed: failedMetrics.length === 0,
    basePassed: baseFailedMetrics.length === 0,
    canaryThresholdPassed,
    failedMetrics: [...new Set(failedMetrics)],
    baseFailedMetrics: [...new Set(baseFailedMetrics)],
    metrics,
  };
}

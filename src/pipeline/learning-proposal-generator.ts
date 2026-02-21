import { getPool } from "../db/client.js";
import { loadTaxonomy } from "../taxonomy/load.js";
import { tokenize } from "../utils/text.js";
import {
  insertLearningProposals,
  type SelfImprovementBatchSummary,
} from "./persist.js";
import type { SelfImproveProposal, SelfImproveProposalKind } from "../types.js";

interface QaFailRow {
  predicted_category: string;
  corrected_category: string | null;
  review_notes: string | null;
  title: string | null;
  description: string | null;
}

function clampScore(value: number, min = 0, max = 1): number {
  return Math.max(min, Math.min(max, value));
}

function pickTopToken(rows: QaFailRow[], disallowed: Set<string>): string | null {
  const counts = new Map<string, number>();
  for (const row of rows) {
    const text = `${row.title ?? ""} ${row.description ?? ""}`;
    for (const token of tokenize(text)) {
      if (token.length < 3 || /^\d+$/.test(token) || disallowed.has(token)) {
        continue;
      }
      counts.set(token, (counts.get(token) ?? 0) + 1);
    }
  }

  let best: { token: string; count: number } | null = null;
  for (const [token, count] of counts.entries()) {
    if (!best || count > best.count) {
      best = { token, count };
    }
  }
  return best?.token ?? null;
}

async function readQaFailRows(runId?: string | null): Promise<QaFailRow[]> {
  const pool = getPool();
  const result = await pool.query<QaFailRow>(
    `
      SELECT
        feedback.predicted_category,
        feedback.corrected_category,
        feedback.review_notes,
        products.title,
        products.description
      FROM pipeline_qa_feedback AS feedback
      LEFT JOIN products
        ON products.run_id = feedback.run_id
       AND products.source_sku = feedback.source_sku
      WHERE feedback.review_status = 'fail'
        AND ($1::uuid IS NULL OR feedback.run_id = $1)
      ORDER BY feedback.imported_at DESC
      LIMIT 500
    `,
    [runId ?? null],
  );
  return result.rows;
}

function createThresholdProposal(input: {
  kind: SelfImproveProposalKind;
  targetSlug: string;
  field: "auto_min_confidence" | "auto_min_margin";
  value: number;
  reason: string;
  confidenceScore: number;
  expectedImpactScore: number;
}): {
  proposalKind: SelfImproveProposalKind;
  confidenceScore: number;
  expectedImpactScore: number;
  payload: {
    target_slug: string;
    field: "auto_min_confidence" | "auto_min_margin";
    action: "set";
    value: number;
    reason: string;
  };
  source: Record<string, unknown>;
} {
  return {
    proposalKind: input.kind,
    confidenceScore: input.confidenceScore,
    expectedImpactScore: input.expectedImpactScore,
    payload: {
      target_slug: input.targetSlug,
      field: input.field,
      action: "set",
      value: input.value,
      reason: input.reason,
    },
    source: {
      strategy: "gate_metric_adjustment",
      reason: input.reason,
    },
  };
}

export async function generateLearningProposals(input: {
  batchId?: string | null;
  runId?: string | null;
  failedGateMetrics?: string[];
  topConfusionAlerts?: Array<{
    category_slug: string;
    affected_count: number;
    low_margin_count: number;
    contradiction_count: number;
    fallback_count: number;
  }>;
  maxProposals?: number;
}): Promise<{
  proposals: SelfImproveProposal[];
  summaryPatch: Partial<SelfImprovementBatchSummary>;
}> {
  const taxonomy = loadTaxonomy();
  const qaRows = await readQaFailRows(input.runId ?? null);
  const generated: Array<{
    proposalKind: SelfImproveProposalKind;
    confidenceScore: number;
    expectedImpactScore: number;
    payload: {
      target_slug: string;
      field:
        | "include_any"
        | "exclude_any"
        | "strong_exclude_any"
        | "auto_min_confidence"
        | "auto_min_margin";
      action: "add" | "remove" | "set";
      value: string | number;
      reason: string;
    };
    source?: Record<string, unknown>;
  }> = [];

  const rowsByCorrected = new Map<string, QaFailRow[]>();
  for (const row of qaRows) {
    const corrected = row.corrected_category?.trim();
    if (!corrected) {
      continue;
    }
    const existing = rowsByCorrected.get(corrected) ?? [];
    existing.push(row);
    rowsByCorrected.set(corrected, existing);
  }

  for (const [correctedCategory, rows] of rowsByCorrected.entries()) {
    const rule = taxonomy.rulesBySlug.get(correctedCategory);
    if (!rule) {
      continue;
    }

    const disallowed = new Set<string>([
      ...rule.include_any.map((item) => item.toLowerCase()),
      ...rule.exclude_any.map((item) => item.toLowerCase()),
      ...rule.strong_exclude_any.map((item) => item.toLowerCase()),
    ]);
    const token = pickTopToken(rows, disallowed);
    if (!token) {
      continue;
    }

    generated.push({
      proposalKind: "rule_term_add",
      confidenceScore: clampScore(0.6 + rows.length * 0.02),
      expectedImpactScore: clampScore(rows.length / 25),
      payload: {
        target_slug: correctedCategory,
        field: "include_any",
        action: "add",
        value: token,
        reason: "qa_fail_correction_signal",
      },
      source: {
        strategy: "qa_feedback_term_mining",
        corrected_category: correctedCategory,
        fail_count: rows.length,
      },
    });
  }

  const failedMetrics = new Set(input.failedGateMetrics ?? []);
  const alerts = [...(input.topConfusionAlerts ?? [])].sort(
    (left, right) =>
      right.contradiction_count + right.low_margin_count - (left.contradiction_count + left.low_margin_count),
  );
  const topAlert = alerts[0];

  if (failedMetrics.has("auto_accepted_rate") && topAlert) {
    const rule = taxonomy.rulesBySlug.get(topAlert.category_slug);
    if (rule) {
      const current = typeof rule.auto_min_confidence === "number" ? rule.auto_min_confidence : 0.76;
      generated.push(
        createThresholdProposal({
          kind: "threshold_tune",
          targetSlug: topAlert.category_slug,
          field: "auto_min_confidence",
          value: clampScore(current - 0.01, 0.55, 0.98),
          reason: "raise_auto_acceptance",
          confidenceScore: 0.72,
          expectedImpactScore: 0.5,
        }),
      );
    }
  }

  if (failedMetrics.has("needs_review_rate") && topAlert) {
    const rule = taxonomy.rulesBySlug.get(topAlert.category_slug);
    if (rule) {
      const current = typeof rule.auto_min_margin === "number" ? rule.auto_min_margin : 0.1;
      generated.push(
        createThresholdProposal({
          kind: "threshold_tune",
          targetSlug: topAlert.category_slug,
          field: "auto_min_margin",
          value: clampScore(current - 0.01, 0.04, 0.4),
          reason: "reduce_review_pressure",
          confidenceScore: 0.7,
          expectedImpactScore: 0.45,
        }),
      );
    }
  }

  if (failedMetrics.has("fallback_category_rate")) {
    const fallbackRule = taxonomy.rulesBySlug.get("outros_escolares");
    if (fallbackRule) {
      const current =
        typeof fallbackRule.auto_min_confidence === "number"
          ? fallbackRule.auto_min_confidence
          : 0.86;
      generated.push(
        createThresholdProposal({
          kind: "threshold_tune",
          targetSlug: "outros_escolares",
          field: "auto_min_confidence",
          value: clampScore(current + 0.01, 0.5, 0.98),
          reason: "contain_fallback_expansion",
          confidenceScore: 0.74,
          expectedImpactScore: 0.52,
        }),
      );
    }
  }

  for (const alert of alerts.slice(0, 2)) {
    const structuralPressure = alert.low_margin_count + alert.contradiction_count;
    if (structuralPressure < 4) {
      continue;
    }
    generated.push({
      proposalKind: "taxonomy_merge",
      confidenceScore: clampScore(0.45 + structuralPressure * 0.03),
      expectedImpactScore: clampScore(structuralPressure / 20),
      payload: {
        target_slug: alert.category_slug,
        field: "include_any",
        action: "add",
        value: "structural_merge_probe",
        reason: "high_confusion_structural_signal",
      },
      source: {
        strategy: "confusion_structural_signal",
        alert,
      },
    });
  }

  const maxProposals = Math.max(1, Math.floor(input.maxProposals ?? 30));
  const limited = generated
    .sort((left, right) => right.expectedImpactScore - left.expectedImpactScore)
    .slice(0, maxProposals);

  const proposals = await insertLearningProposals({
    batchId: input.batchId ?? null,
    runId: input.runId ?? null,
    proposals: limited,
  });

  return {
    proposals,
    summaryPatch: {
      proposals_generated: proposals.length,
    },
  };
}

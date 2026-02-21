import { createHash } from "node:crypto";
import { getConfig } from "../config.js";
import { getPool } from "../db/client.js";
import type { HarnessEvalResult } from "../types.js";
import {
  createBenchmarkSnapshot,
  getBenchmarkSnapshotById,
  getLatestBenchmarkSnapshot,
  getPipelineRunById,
  listPipelineRunsByStore,
  type BenchmarkSnapshot,
} from "./persist.js";

function asNumber(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function buildHash(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function metricDelta(candidate: number | null, baseline: number | null): number {
  if (candidate === null || baseline === null) {
    return 0;
  }
  return candidate - baseline;
}

async function buildBenchmarkSource(input: {
  storeId: string;
}): Promise<{
  source: Record<string, unknown>;
  rowCount: number;
  sampleSize: number;
}> {
  const pool = getPool();
  const qaResult = await pool.query<{ reviewed_count: number; fail_count: number; pass_count: number }>(
    `
      SELECT
        COUNT(*) FILTER (WHERE feedback.review_status IN ('pass', 'fail'))::int AS reviewed_count,
        COUNT(*) FILTER (WHERE feedback.review_status = 'fail')::int AS fail_count,
        COUNT(*) FILTER (WHERE feedback.review_status = 'pass')::int AS pass_count
      FROM pipeline_qa_feedback AS feedback
      JOIN pipeline_runs AS run
        ON run.id = feedback.run_id
      WHERE run.store_id = $1
    `,
    [input.storeId],
  );

  const hardCaseResult = await pool.query<{ hard_case_count: number }>(
    `
      SELECT
        COALESCE(SUM(jsonb_array_length(COALESCE(stats_json->'top_confusion_alerts', '[]'::jsonb))), 0)::int AS hard_case_count
      FROM (
        SELECT stats_json
        FROM pipeline_runs
        WHERE store_id = $1
          AND status IN ('completed_pending_review', 'accepted', 'rejected')
        ORDER BY started_at DESC
        LIMIT 15
      ) AS recent
    `,
    [input.storeId],
  );

  const reviewedCount = Number(qaResult.rows[0]?.reviewed_count ?? 0);
  const failCount = Number(qaResult.rows[0]?.fail_count ?? 0);
  const passCount = Number(qaResult.rows[0]?.pass_count ?? 0);
  const hardCaseCount = Number(hardCaseResult.rows[0]?.hard_case_count ?? 0);
  const rowCount = reviewedCount + hardCaseCount;
  const sampleSize = rowCount;

  return {
    source: {
      strategy: "qa_feedback_plus_hard_cases",
      qa_reviewed_count: reviewedCount,
      qa_fail_count: failCount,
      qa_pass_count: passCount,
      hard_case_count: hardCaseCount,
    },
    rowCount,
    sampleSize,
  };
}

export async function buildHarnessBenchmarkSnapshot(input: {
  storeId: string;
}): Promise<BenchmarkSnapshot> {
  const benchmark = await buildBenchmarkSource({
    storeId: input.storeId,
  });

  return createBenchmarkSnapshot({
    storeId: input.storeId,
    source: benchmark.source,
    rowCount: benchmark.rowCount,
    sampleSize: benchmark.sampleSize,
    datasetHash: buildHash({
      storeId: input.storeId,
      source: benchmark.source,
      rowCount: benchmark.rowCount,
      sampleSize: benchmark.sampleSize,
    }),
  });
}

function readAccuracy(stats: Record<string, unknown>, key: string): number | null {
  const value = asNumber(stats[key]);
  if (value === null) {
    return null;
  }
  return value;
}

export async function evaluateHarnessForRun(input: {
  storeId: string;
  candidateRunId: string;
  baselineRunId?: string | null;
  benchmarkSnapshotId?: string | null;
}): Promise<{
  result: HarnessEvalResult;
  benchmarkSnapshot: BenchmarkSnapshot;
}> {
  const config = getConfig();
  const candidateRun = await getPipelineRunById(input.candidateRunId);
  if (!candidateRun) {
    throw new Error(`Candidate run ${input.candidateRunId} not found for harness evaluation.`);
  }

  let baselineRunId = input.baselineRunId ?? null;
  if (!baselineRunId) {
    const recentRuns = await listPipelineRunsByStore({
      storeId: input.storeId,
      limit: 20,
    });
    const baseline = recentRuns.find(
      (run) => run.runId !== input.candidateRunId && run.status !== "failed",
    );
    baselineRunId = baseline?.runId ?? null;
  }

  const baselineRun = baselineRunId ? await getPipelineRunById(baselineRunId) : null;
  const benchmarkSnapshot =
    (input.benchmarkSnapshotId
      ? await getBenchmarkSnapshotById(input.benchmarkSnapshotId)
      : null) ?? (await getLatestBenchmarkSnapshot(input.storeId)) ??
    (await buildHarnessBenchmarkSnapshot({ storeId: input.storeId }));

  const candidateStats = candidateRun.stats ?? {};
  const baselineStats = baselineRun?.stats ?? {};
  const failedMetrics: string[] = [];

  const candidateFallbackRate = asNumber(candidateStats.fallback_category_rate);
  const candidateNeedsReviewRate = asNumber(candidateStats.needs_review_rate);

  if (benchmarkSnapshot.sampleSize < config.SELF_IMPROVE_GATE_MIN_SAMPLE_SIZE) {
    failedMetrics.push("benchmark_sample_size");
  }

  if (
    candidateFallbackRate !== null &&
    candidateFallbackRate > config.HARNESS_MAX_FALLBACK_RATE
  ) {
    failedMetrics.push("fallback_category_rate");
  }

  if (
    candidateNeedsReviewRate !== null &&
    candidateNeedsReviewRate > config.HARNESS_MAX_NEEDS_REVIEW_RATE
  ) {
    failedMetrics.push("needs_review_rate");
  }

  const l1Candidate = readAccuracy(candidateStats, "l1_accuracy");
  const l1Baseline = readAccuracy(baselineStats, "l1_accuracy");
  const l2Candidate = readAccuracy(candidateStats, "l2_accuracy");
  const l2Baseline = readAccuracy(baselineStats, "l2_accuracy");
  const l3Candidate = readAccuracy(candidateStats, "l3_accuracy");
  const l3Baseline = readAccuracy(baselineStats, "l3_accuracy");

  const l1Delta = metricDelta(l1Candidate, l1Baseline);
  const l2Delta = metricDelta(l2Candidate, l2Baseline);
  const l3Delta = metricDelta(l3Candidate, l3Baseline);

  if (l1Candidate !== null && l1Baseline !== null && l1Delta < config.HARNESS_MIN_L1_DELTA) {
    failedMetrics.push("l1_delta");
  }
  if (l2Candidate !== null && l2Baseline !== null && l2Delta < config.HARNESS_MIN_L2_DELTA) {
    failedMetrics.push("l2_delta");
  }
  if (l3Candidate !== null && l3Baseline !== null && l3Delta < config.HARNESS_MIN_L3_DELTA) {
    failedMetrics.push("l3_delta");
  }

  const metricScores: Record<string, number> = {
    benchmark_sample_size: benchmarkSnapshot.sampleSize,
    candidate_fallback_category_rate: candidateFallbackRate ?? 0,
    candidate_needs_review_rate: candidateNeedsReviewRate ?? 0,
    l1_delta: l1Delta,
    l2_delta: l2Delta,
    l3_delta: l3Delta,
  };

  return {
    benchmarkSnapshot,
    result: {
      passed: failedMetrics.length === 0,
      metricScores,
      failedMetrics: [...new Set(failedMetrics)],
      baselineRunId,
      candidateRunId: input.candidateRunId,
    },
  };
}

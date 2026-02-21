import path from "node:path";
import { getConfig, type AppConfig } from "../config.js";
import { buildCanarySubset, writeCanaryState } from "../canary/select-subset.js";
import { runPipeline } from "./run.js";
import {
  getPipelineRunById,
  recordHarnessRun,
  type SelfImprovementBatchListItem,
} from "./persist.js";
import { buildSelfCorrectionContext, evaluateQualityGateFromStats } from "./self-correction-context.js";
import { generateLearningProposals } from "./learning-proposal-generator.js";
import { evaluateHarnessForRun } from "./harness.js";
import { applyLearningProposals } from "./learning-apply.js";
import { rollbackOnHarnessDegrade } from "./learning-rollback.js";
import type { SelfImproveLoopType } from "../types.js";

interface ParsedAlert {
  category_slug: string;
  affected_count: number;
  low_margin_count: number;
  contradiction_count: number;
  fallback_count: number;
}

interface LoopRunOutput {
  runId: string;
  storeId: string;
  stats: Record<string, unknown>;
}

interface RequiredPipelineEnv {
  inputPath: string;
  storeId: string;
  outputDir: string;
  canarySubsetPath: string;
  canaryStatePath: string;
  canarySampleSize: number;
  canaryFixedRatio: number;
  canaryRandomSeed: string;
  canaryAutoAcceptThreshold: number;
}

export interface LoopAttemptResult {
  runId: string;
  passed: boolean;
  retryableFailure: boolean;
  qualityGate: {
    passed: boolean;
    failedMetrics: string[];
    metrics: Record<string, unknown>;
  };
  harnessPassed: boolean;
  failedMetrics: string[];
  correctionContext: ReturnType<typeof buildSelfCorrectionContext> | null;
  learningResult: Record<string, unknown>;
  harnessDelta: number;
}

function asNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function clampConfidence(value: number): number {
  return Math.max(0, Math.min(1, value));
}

function parseAlerts(value: unknown): ParsedAlert[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .filter((entry) => typeof entry === "object" && entry !== null)
    .map((entry) => entry as Record<string, unknown>)
    .map((entry) => ({
      category_slug: String(entry.category_slug ?? ""),
      affected_count: asNumber(entry.affected_count),
      low_margin_count: asNumber(entry.low_margin_count),
      contradiction_count: asNumber(entry.contradiction_count),
      fallback_count: asNumber(entry.fallback_count),
    }))
    .filter((entry) => entry.category_slug.length > 0);
}

function buildRunLabel(input: {
  loopType: SelfImproveLoopType;
  batchId: string;
  sequenceNo: number;
  attemptNo: number;
}): string {
  return [
    "self-improve",
    input.loopType,
    input.batchId,
    `seq${input.sequenceNo}`,
    `attempt${input.attemptNo}`,
    new Date().toISOString().replace(/[:.]/g, "-"),
  ].join("-");
}

function getRequiredPipelineEnv(config: AppConfig): RequiredPipelineEnv {
  if (!config.CATALOG_INPUT_PATH) {
    throw new Error("Missing CATALOG_INPUT_PATH in environment.");
  }
  if (!config.STORE_ID) {
    throw new Error("Missing STORE_ID in environment.");
  }

  const cwd = process.cwd();
  return {
    inputPath: path.resolve(cwd, config.CATALOG_INPUT_PATH),
    storeId: config.STORE_ID,
    outputDir: path.resolve(cwd, config.OUTPUT_DIR),
    canarySubsetPath: path.resolve(cwd, config.CANARY_SUBSET_PATH),
    canaryStatePath: path.resolve(cwd, config.CANARY_STATE_PATH),
    canarySampleSize: config.CANARY_SAMPLE_SIZE,
    canaryFixedRatio: config.CANARY_FIXED_RATIO,
    canaryRandomSeed: config.CANARY_RANDOM_SEED,
    canaryAutoAcceptThreshold: config.CANARY_AUTO_ACCEPT_THRESHOLD,
  };
}

async function executeFullLoop(input: {
  batchId: string;
  sequenceNo: number;
  attemptNo: number;
  env: RequiredPipelineEnv;
}): Promise<LoopRunOutput> {
  const summary = await runPipeline({
    inputPath: input.env.inputPath,
    storeId: input.env.storeId,
    runLabel: buildRunLabel({
      loopType: "full",
      batchId: input.batchId,
      sequenceNo: input.sequenceNo,
      attemptNo: input.attemptNo,
    }),
  });

  const runRecord = await getPipelineRunById(summary.runId);
  if (!runRecord) {
    throw new Error(`Could not load run stats for full run ${summary.runId}.`);
  }

  return {
    runId: summary.runId,
    storeId: runRecord.storeId,
    stats: runRecord.stats,
  };
}

async function executeCanaryLoop(input: {
  batchId: string;
  sequenceNo: number;
  attemptNo: number;
  env: RequiredPipelineEnv;
}): Promise<LoopRunOutput> {
  const subset = await buildCanarySubset({
    inputPath: input.env.inputPath,
    outputDir: input.env.outputDir,
    subsetPath: input.env.canarySubsetPath,
    statePath: input.env.canaryStatePath,
    sampleSize: input.env.canarySampleSize,
    fixedRatio: input.env.canaryFixedRatio,
    randomSeed: input.env.canaryRandomSeed,
    storeId: input.env.storeId,
  });

  const summary = await runPipeline({
    inputPath: subset.subsetPath,
    storeId: input.env.storeId,
    runLabel: buildRunLabel({
      loopType: "canary",
      batchId: input.batchId,
      sequenceNo: input.sequenceNo,
      attemptNo: input.attemptNo,
    }),
  });

  const confusionArtifact = summary.artifacts.find(
    (artifact) => artifact.format === "confusion-csv" || artifact.key === "confusion_hotlist_csv",
  );
  if (!confusionArtifact) {
    throw new Error(`Canary run ${summary.runId} did not produce a confusion hotlist artifact.`);
  }

  await writeCanaryState({
    statePath: input.env.canaryStatePath,
    runId: summary.runId,
    hotlistPath: path.resolve(input.env.outputDir, confusionArtifact.fileName),
  });

  const runRecord = await getPipelineRunById(summary.runId);
  if (!runRecord) {
    throw new Error(`Could not load run stats for canary run ${summary.runId}.`);
  }

  return {
    runId: summary.runId,
    storeId: runRecord.storeId,
    stats: runRecord.stats,
  };
}

async function executeLoopByType(input: {
  loopType: SelfImproveLoopType;
  batchId: string;
  sequenceNo: number;
  attemptNo: number;
  env: RequiredPipelineEnv;
}): Promise<LoopRunOutput> {
  if (input.loopType === "canary") {
    return executeCanaryLoop({
      batchId: input.batchId,
      sequenceNo: input.sequenceNo,
      attemptNo: input.attemptNo,
      env: input.env,
    });
  }

  return executeFullLoop({
    batchId: input.batchId,
    sequenceNo: input.sequenceNo,
    attemptNo: input.attemptNo,
    env: input.env,
  });
}

function computeHarnessDelta(metricScores: Record<string, number>): number {
  const l1 = asNumber(metricScores.l1_delta);
  const l2 = asNumber(metricScores.l2_delta);
  const l3 = asNumber(metricScores.l3_delta);
  return (l1 + l2 + l3) / 3;
}

function hasHighSeveritySchemaViolations(
  proposals: Array<{
    payload: {
      target_slug?: string;
      field?:
        | "include_any"
        | "exclude_any"
        | "strong_exclude_any"
        | "auto_min_confidence"
        | "auto_min_margin";
      value?: string | number;
      reason?: string;
    };
  }>,
): boolean {
  return proposals.some((proposal) => {
    const payload = proposal.payload;
    const targetSlug = String(payload.target_slug ?? "").trim();
    const reason = String(payload.reason ?? "").trim();
    const field = payload.field;
    const value = payload.value;

    if (targetSlug.length === 0 || reason.length === 0 || !field) {
      return true;
    }
    if (
      (field === "auto_min_confidence" || field === "auto_min_margin") &&
      typeof value !== "number"
    ) {
      return true;
    }
    if (
      (field === "include_any" || field === "exclude_any" || field === "strong_exclude_any") &&
      typeof value !== "string"
    ) {
      return true;
    }
    return false;
  });
}

export async function runLoopAttempt(input: {
  batch: SelfImprovementBatchListItem;
  sequenceNo: number;
  attemptNo: number;
  previousFailedMetrics: string[];
}): Promise<LoopAttemptResult> {
  const config = getConfig();
  const env = getRequiredPipelineEnv(config);
  const canaryRetryDegradeMode =
    config.SELF_IMPROVE_CANARY_RETRY_DEGRADE_ENABLED &&
    input.batch.loopType === "canary" &&
    input.attemptNo > 1;
  const proposalMinConfidence = canaryRetryDegradeMode
    ? clampConfidence(config.SELF_IMPROVE_CANARY_RETRY_MIN_PROPOSAL_CONFIDENCE)
    : 0;
  const structuralProposalsAllowed = !canaryRetryDegradeMode;

  const loopRun = await executeLoopByType({
    loopType: input.batch.loopType,
    batchId: input.batch.id,
    sequenceNo: input.sequenceNo,
    attemptNo: input.attemptNo,
    env,
  });

  const qualityGate = evaluateQualityGateFromStats({
    stats: loopRun.stats,
    requireCanaryThreshold: input.batch.loopType === "canary",
    canaryThreshold: input.batch.loopType === "canary" ? env.canaryAutoAcceptThreshold : undefined,
  });

  const alerts = parseAlerts(loopRun.stats.top_confusion_alerts);
  const mergedFailedMetrics = [...new Set([...qualityGate.failedMetrics, ...input.previousFailedMetrics])];

  const proposalResult = await generateLearningProposals({
    batchId: input.batch.id,
    runId: loopRun.runId,
    failedGateMetrics: mergedFailedMetrics,
    topConfusionAlerts: alerts,
    maxProposals: 40,
    minConfidenceScore: proposalMinConfidence,
    allowStructuralProposals: structuralProposalsAllowed,
  });
  const highSeveritySchemaViolations = hasHighSeveritySchemaViolations(proposalResult.proposals);

  const harnessEvaluation = await evaluateHarnessForRun({
    storeId: loopRun.storeId,
    candidateRunId: loopRun.runId,
  });

  await recordHarnessRun({
    batchId: input.batch.id,
    runId: loopRun.runId,
    benchmarkSnapshotId: harnessEvaluation.benchmarkSnapshot.id,
    result: harnessEvaluation.result,
  });

  const shouldAutoApply =
    input.batch.autoApplyPolicy === "if_gate_passes" &&
    qualityGate.passed &&
    harnessEvaluation.result.passed &&
    !highSeveritySchemaViolations;

  const applyResult = shouldAutoApply
    ? await applyLearningProposals({
        batchId: input.batch.id,
        runId: loopRun.runId,
        harnessResult: harnessEvaluation.result,
        maxStructuralChangesPerLoop: canaryRetryDegradeMode
          ? 0
          : config.SELF_IMPROVE_MAX_STRUCTURAL_CHANGES_PER_LOOP,
      })
    : {
        considered: 0,
        applied: 0,
        structuralApplied: 0,
      };

  const rollbackResult = await rollbackOnHarnessDegrade({
    batchId: input.batch.id,
    runId: loopRun.runId,
    harnessResult: harnessEvaluation.result,
    watchLoops: config.SELF_IMPROVE_POST_APPLY_WATCH_LOOPS,
    rollbackOnDegrade: config.SELF_IMPROVE_ROLLBACK_ON_DEGRADE,
  });

  const passed = qualityGate.passed && harnessEvaluation.result.passed;
  const failedMetrics = [...new Set([...mergedFailedMetrics, ...harnessEvaluation.result.failedMetrics])];

  return {
    runId: loopRun.runId,
    passed,
    retryableFailure: false,
    qualityGate,
    harnessPassed: harnessEvaluation.result.passed,
    failedMetrics,
    correctionContext: passed
      ? null
      : buildSelfCorrectionContext({
          error: new Error("Self-improvement loop failed one or more gates."),
          stats: loopRun.stats,
        }),
    harnessDelta: computeHarnessDelta(harnessEvaluation.result.metricScores),
    learningResult: {
      proposals_generated: proposalResult.proposals.length,
      proposals_applied: applyResult.applied,
      structural_applies: applyResult.structuralApplied,
      auto_applied_updates: applyResult.applied,
      rollback_triggered: rollbackResult.rolledBack,
      rollback_change_id: rollbackResult.change?.id ?? null,
      gate_failed_metrics: failedMetrics,
      harness_passed: harnessEvaluation.result.passed,
      quality_gate_passed: qualityGate.passed,
      benchmark_snapshot_id: harnessEvaluation.benchmarkSnapshot.id,
      harness_failed_metrics: harnessEvaluation.result.failedMetrics,
      harness_metric_scores: harnessEvaluation.result.metricScores,
      high_severity_schema_violations: highSeveritySchemaViolations,
      candidate_fixes: proposalResult.proposals.map((proposal) => proposal.payload.reason),
      canary_retry_degrade_mode: canaryRetryDegradeMode,
      proposal_min_confidence: proposalMinConfidence,
      structural_proposals_allowed: structuralProposalsAllowed,
    },
  };
}

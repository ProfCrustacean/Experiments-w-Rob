import type { SelfImproveBatchStatus, SelfImproveRunStatus } from "../types.js";
import {
  claimNextQueuedSelfImprovementBatch,
  finalizeSelfImprovementBatch,
  finalizeSelfImprovementRunAttempt,
  getSelfImprovementBatchDetails,
  startSelfImprovementRunAttempt,
  updateSelfImprovementBatchSummary,
  type SelfImprovementBatchListItem,
  type SelfImprovementBatchSummary,
  type SelfImprovementRunItem,
} from "./persist.js";
import { buildSelfCorrectionContext } from "./self-correction-context.js";
import { runLoopAttempt, type LoopAttemptResult } from "./self-improvement-loop.js";
import {
  parseSelfImprovementPhrase,
  type SelfImprovementPhraseIntent,
} from "./self-improvement-phrase.js";

interface SequenceResult {
  succeeded: boolean;
  retriedSuccess: boolean;
  finalFailed: boolean;
  failedMetrics: string[];
  proposalsGenerated: number;
  proposalsApplied: number;
  structuralApplied: number;
  autoAppliedUpdates: number;
  rollbacksTriggered: number;
  harnessDeltaTotal: number;
  harnessDeltaSamples: number;
}

export type ProcessNextSelfImprovementBatchResult =
  | {
      status: "idle";
    }
  | {
      status: "processed";
      batchId: string;
      batchStatus: SelfImproveBatchStatus;
      summary: SelfImprovementBatchSummary;
    };

function asNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function hasRunErrorMessage(run: SelfImprovementRunItem): boolean {
  const message = asRecord(run.error).message;
  return typeof message === "string" && message.trim().length > 0;
}

function isNonRetryableGateFailure(run: SelfImprovementRunItem): boolean {
  const gatePassed = asRecord(run.gateResult).passed;
  return gatePassed === false && !hasRunErrorMessage(run);
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error ?? "unknown_error");
}

function defaultBatchSummary(batch: SelfImprovementBatchListItem): SelfImprovementBatchSummary {
  const source = batch.summary ?? {};
  return {
    ...source,
    total_loops: asNumber(source.total_loops) || batch.requestedCount,
    completed_loops: asNumber(source.completed_loops),
    failed_loops: asNumber(source.failed_loops),
    running_sequence: source.running_sequence ?? null,
    success_count: asNumber(source.success_count),
    retried_success_count: asNumber(source.retried_success_count),
    final_failed_count: asNumber(source.final_failed_count),
    gate_pass_rate: asNumber(source.gate_pass_rate),
    auto_applied_updates_count: asNumber(source.auto_applied_updates_count),
    proposals_generated: asNumber(source.proposals_generated),
    proposals_applied: asNumber(source.proposals_applied),
    structural_applies: asNumber(source.structural_applies),
    rollbacks_triggered: asNumber(source.rollbacks_triggered),
    avg_harness_delta: asNumber(source.avg_harness_delta),
  };
}

function withSummaryIncrements(
  summary: SelfImprovementBatchSummary,
  sequenceResult: SequenceResult,
): SelfImprovementBatchSummary {
  const completedLoops = asNumber(summary.completed_loops) + 1;
  const successCount = asNumber(summary.success_count) + (sequenceResult.succeeded ? 1 : 0);
  const retriedSuccessCount =
    asNumber(summary.retried_success_count) + (sequenceResult.retriedSuccess ? 1 : 0);
  const finalFailedCount = asNumber(summary.final_failed_count) + (sequenceResult.finalFailed ? 1 : 0);
  const gatePasses = successCount + retriedSuccessCount;

  const currentHarnessSamples = asNumber((summary as Record<string, unknown>).harness_delta_samples);
  const nextHarnessSamples = currentHarnessSamples + sequenceResult.harnessDeltaSamples;
  const currentHarnessTotal =
    asNumber((summary as Record<string, unknown>).harness_delta_total) +
    sequenceResult.harnessDeltaTotal;

  return {
    ...summary,
    completed_loops: completedLoops,
    failed_loops: finalFailedCount,
    success_count: successCount,
    retried_success_count: retriedSuccessCount,
    final_failed_count: finalFailedCount,
    gate_pass_rate: completedLoops > 0 ? gatePasses / completedLoops : 0,
    auto_applied_updates_count:
      asNumber(summary.auto_applied_updates_count) + sequenceResult.autoAppliedUpdates,
    proposals_generated: asNumber(summary.proposals_generated) + sequenceResult.proposalsGenerated,
    proposals_applied: asNumber(summary.proposals_applied) + sequenceResult.proposalsApplied,
    structural_applies: asNumber(summary.structural_applies) + sequenceResult.structuralApplied,
    rollbacks_triggered: asNumber(summary.rollbacks_triggered) + sequenceResult.rollbacksTriggered,
    avg_harness_delta: nextHarnessSamples > 0 ? currentHarnessTotal / nextHarnessSamples : 0,
    harness_delta_samples: nextHarnessSamples,
    harness_delta_total: currentHarnessTotal,
  };
}

interface SequenceRuntimePlan {
  shouldProcess: boolean;
  completed: boolean;
  succeeded: boolean;
  retriedSuccess: boolean;
  finalFailed: boolean;
  nextAttemptNo: number;
  latestRun: SelfImprovementRunItem | null;
}

function getLatestRunForSequence(runs: SelfImprovementRunItem[]): SelfImprovementRunItem | null {
  if (runs.length === 0) {
    return null;
  }

  return [...runs].sort((left, right) => right.attemptNo - left.attemptNo)[0];
}

function buildSequenceRuntimePlan(
  runs: SelfImprovementRunItem[],
  retryLimit: number,
): SequenceRuntimePlan {
  const latestRun = getLatestRunForSequence(runs);
  if (!latestRun) {
    return {
      shouldProcess: true,
      completed: false,
      succeeded: false,
      retriedSuccess: false,
      finalFailed: false,
      nextAttemptNo: 1,
      latestRun: null,
    };
  }

  if (latestRun.status === "succeeded") {
    return {
      shouldProcess: false,
      completed: true,
      succeeded: true,
      retriedSuccess: false,
      finalFailed: false,
      nextAttemptNo: latestRun.attemptNo,
      latestRun,
    };
  }

  if (latestRun.status === "retried_succeeded") {
    return {
      shouldProcess: false,
      completed: true,
      succeeded: false,
      retriedSuccess: true,
      finalFailed: false,
      nextAttemptNo: latestRun.attemptNo,
      latestRun,
    };
  }

  if (latestRun.status === "retried_failed") {
    return {
      shouldProcess: false,
      completed: true,
      succeeded: false,
      retriedSuccess: false,
      finalFailed: true,
      nextAttemptNo: latestRun.attemptNo,
      latestRun,
    };
  }

  if (latestRun.status === "failed") {
    const hasRetryRemaining =
      !isNonRetryableGateFailure(latestRun) && latestRun.attemptNo <= retryLimit;
    if (!hasRetryRemaining) {
      return {
        shouldProcess: false,
        completed: true,
        succeeded: false,
        retriedSuccess: false,
        finalFailed: true,
        nextAttemptNo: latestRun.attemptNo,
        latestRun,
      };
    }

    return {
      shouldProcess: true,
      completed: false,
      succeeded: false,
      retriedSuccess: false,
      finalFailed: false,
      nextAttemptNo: latestRun.attemptNo + 1,
      latestRun,
    };
  }

  return {
    shouldProcess: true,
    completed: false,
    succeeded: false,
    retriedSuccess: false,
    finalFailed: false,
    nextAttemptNo: latestRun.attemptNo,
    latestRun,
  };
}

function rebuildSummaryFromRuns(batch: SelfImprovementBatchListItem, runs: SelfImprovementRunItem[]) {
  const source = defaultBatchSummary(batch);
  const runsBySequence = new Map<number, SelfImprovementRunItem[]>();

  for (const run of runs) {
    const current = runsBySequence.get(run.sequenceNo) ?? [];
    current.push(run);
    runsBySequence.set(run.sequenceNo, current);
  }

  let completedLoops = 0;
  let successCount = 0;
  let retriedSuccessCount = 0;
  let finalFailedCount = 0;
  let runningSequence: number | null = null;
  let autoAppliedUpdatesCount = 0;
  let proposalsGenerated = 0;
  let proposalsApplied = 0;
  let structuralApplies = 0;
  let rollbacksTriggered = 0;

  for (let sequenceNo = 1; sequenceNo <= batch.requestedCount; sequenceNo += 1) {
    const sequenceRuns = runsBySequence.get(sequenceNo) ?? [];
    const plan = buildSequenceRuntimePlan(sequenceRuns, batch.retryLimit);
    const latest = plan.latestRun;

    if (latest?.status === "running" && runningSequence === null) {
      runningSequence = sequenceNo;
    }

    if (!plan.completed) {
      continue;
    }

    completedLoops += 1;
    successCount += plan.succeeded ? 1 : 0;
    retriedSuccessCount += plan.retriedSuccess ? 1 : 0;
    finalFailedCount += plan.finalFailed ? 1 : 0;

    autoAppliedUpdatesCount += asNumber(latest?.learningResult.auto_applied_updates);
    proposalsGenerated += asNumber(latest?.learningResult.proposals_generated);
    proposalsApplied += asNumber(latest?.learningResult.proposals_applied);
    structuralApplies += asNumber(latest?.learningResult.structural_applies);
    rollbacksTriggered += latest?.learningResult.rollback_triggered ? 1 : 0;
  }

  return {
    ...source,
    total_loops: batch.requestedCount,
    completed_loops: completedLoops,
    failed_loops: finalFailedCount,
    running_sequence: runningSequence,
    success_count: successCount,
    retried_success_count: retriedSuccessCount,
    final_failed_count: finalFailedCount,
    gate_pass_rate:
      completedLoops > 0 ? (successCount + retriedSuccessCount) / completedLoops : 0,
    auto_applied_updates_count: autoAppliedUpdatesCount,
    proposals_generated: proposalsGenerated,
    proposals_applied: proposalsApplied,
    structural_applies: structuralApplies,
    rollbacks_triggered: rollbacksTriggered,
  } satisfies SelfImprovementBatchSummary;
}

async function processSequence(input: {
  batch: SelfImprovementBatchListItem;
  sequenceNo: number;
  retryLimit: number;
  startAttemptNo: number;
}): Promise<SequenceResult> {
  let priorFailedMetrics: string[] = [];

  for (let attemptNo = input.startAttemptNo; attemptNo <= input.retryLimit + 1; attemptNo += 1) {
    await startSelfImprovementRunAttempt({
      batchId: input.batch.id,
      sequenceNo: input.sequenceNo,
      attemptNo,
    });

    let attemptResult: LoopAttemptResult | null = null;

    try {
      attemptResult = await runLoopAttempt({
        batch: input.batch,
        sequenceNo: input.sequenceNo,
        attemptNo,
        previousFailedMetrics: priorFailedMetrics,
      });

      const shouldRetry =
        !attemptResult.passed && attemptResult.retryableFailure && attemptNo <= input.retryLimit;
      const status: SelfImproveRunStatus = attemptResult.passed
        ? attemptNo === 1
          ? "succeeded"
          : "retried_succeeded"
        : shouldRetry
          ? "failed"
          : attemptNo > 1
            ? "retried_failed"
            : "failed";

      await finalizeSelfImprovementRunAttempt({
        batchId: input.batch.id,
        sequenceNo: input.sequenceNo,
        attemptNo,
        status,
        pipelineRunId: attemptResult.runId,
        gateResult: {
          passed: attemptResult.passed,
          quality_gate_passed: attemptResult.qualityGate.passed,
          harness_passed: attemptResult.harnessPassed,
          failed_metrics: attemptResult.failedMetrics,
          quality_gate_metrics: attemptResult.qualityGate.metrics,
        },
        selfCorrectionContext: attemptResult.correctionContext ?? {},
        learningResult: attemptResult.learningResult,
      });

      if (attemptResult.passed) {
        return {
          succeeded: attemptNo === 1,
          retriedSuccess: attemptNo > 1,
          finalFailed: false,
          failedMetrics: [],
          proposalsGenerated: asNumber(attemptResult.learningResult.proposals_generated),
          proposalsApplied: asNumber(attemptResult.learningResult.proposals_applied),
          structuralApplied: asNumber(attemptResult.learningResult.structural_applies),
          autoAppliedUpdates: asNumber(attemptResult.learningResult.auto_applied_updates),
          rollbacksTriggered: attemptResult.learningResult.rollback_triggered ? 1 : 0,
          harnessDeltaTotal: attemptResult.harnessDelta,
          harnessDeltaSamples: 1,
        };
      }

      if (shouldRetry) {
        priorFailedMetrics = [...new Set([...priorFailedMetrics, ...attemptResult.failedMetrics])];
        continue;
      }

      return {
        succeeded: false,
        retriedSuccess: false,
        finalFailed: true,
        failedMetrics: attemptResult.failedMetrics,
        proposalsGenerated: asNumber(attemptResult.learningResult.proposals_generated),
        proposalsApplied: asNumber(attemptResult.learningResult.proposals_applied),
        structuralApplied: asNumber(attemptResult.learningResult.structural_applies),
        autoAppliedUpdates: asNumber(attemptResult.learningResult.auto_applied_updates),
        rollbacksTriggered: attemptResult.learningResult.rollback_triggered ? 1 : 0,
        harnessDeltaTotal: attemptResult.harnessDelta,
        harnessDeltaSamples: 1,
      };
    } catch (error) {
      const correction = buildSelfCorrectionContext({
        error,
        stats: (attemptResult?.qualityGate.metrics ?? {}) as Record<string, unknown>,
      });
      const shouldRetry = attemptNo <= input.retryLimit;
      const status: SelfImproveRunStatus = shouldRetry
        ? "failed"
        : attemptNo > 1
          ? "retried_failed"
          : "failed";

      await finalizeSelfImprovementRunAttempt({
        batchId: input.batch.id,
        sequenceNo: input.sequenceNo,
        attemptNo,
        status,
        pipelineRunId: attemptResult?.runId ?? null,
        error: {
          message: toErrorMessage(error),
        },
        selfCorrectionContext: correction,
        gateResult: {
          passed: false,
          quality_gate_passed: attemptResult?.qualityGate.passed ?? false,
          harness_passed: attemptResult?.harnessPassed ?? false,
          failed_metrics: correction.failedGateMetrics,
        },
        learningResult: {
          auto_applied_updates: 0,
          proposals_generated: asNumber(attemptResult?.learningResult.proposals_generated),
          proposals_applied: asNumber(attemptResult?.learningResult.proposals_applied),
          structural_applies: asNumber(attemptResult?.learningResult.structural_applies),
          rollback_triggered: Boolean(attemptResult?.learningResult.rollback_triggered),
          candidate_fixes: correction.candidateFixes,
        },
      });

      if (shouldRetry) {
        priorFailedMetrics = [...new Set([...priorFailedMetrics, ...correction.failedGateMetrics])];
        continue;
      }

      return {
        succeeded: false,
        retriedSuccess: false,
        finalFailed: true,
        failedMetrics: correction.failedGateMetrics,
        proposalsGenerated: asNumber(attemptResult?.learningResult.proposals_generated),
        proposalsApplied: asNumber(attemptResult?.learningResult.proposals_applied),
        structuralApplied: asNumber(attemptResult?.learningResult.structural_applies),
        autoAppliedUpdates: 0,
        rollbacksTriggered: attemptResult?.learningResult.rollback_triggered ? 1 : 0,
        harnessDeltaTotal: attemptResult?.harnessDelta ?? 0,
        harnessDeltaSamples: attemptResult ? 1 : 0,
      };
    }
  }

  return {
    succeeded: false,
    retriedSuccess: false,
    finalFailed: true,
    failedMetrics: [],
    proposalsGenerated: 0,
    proposalsApplied: 0,
    structuralApplied: 0,
    autoAppliedUpdates: 0,
    rollbacksTriggered: 0,
    harnessDeltaTotal: 0,
    harnessDeltaSamples: 0,
  };
}

export async function processNextSelfImprovementBatch(): Promise<ProcessNextSelfImprovementBatchResult> {
  const batch = await claimNextQueuedSelfImprovementBatch();
  if (!batch) {
    return {
      status: "idle",
    };
  }

  let summary = defaultBatchSummary(batch);

  try {
    const initialBatch = await getSelfImprovementBatchDetails(batch.id);
    if (initialBatch) {
      summary = rebuildSummaryFromRuns(batch, initialBatch.runs);
      await updateSelfImprovementBatchSummary({
        batchId: batch.id,
        summary,
      });
    }

    for (let sequenceNo = 1; sequenceNo <= batch.requestedCount; sequenceNo += 1) {
      const latestBatch = await getSelfImprovementBatchDetails(batch.id);
      if (latestBatch?.status === "cancelled") {
        summary = {
          ...summary,
          running_sequence: null,
        };

        const cancelled = await finalizeSelfImprovementBatch({
          batchId: batch.id,
          status: "cancelled",
          summary,
        });

        return {
          status: "processed",
          batchId: batch.id,
          batchStatus: cancelled.status,
          summary: cancelled.summary,
        };
      }

      const sequenceRuns = (latestBatch?.runs ?? []).filter((run) => run.sequenceNo === sequenceNo);
      const sequencePlan = buildSequenceRuntimePlan(sequenceRuns, batch.retryLimit);
      if (!sequencePlan.shouldProcess) {
        continue;
      }

      summary = {
        ...summary,
        running_sequence: sequenceNo,
      };

      await updateSelfImprovementBatchSummary({
        batchId: batch.id,
        summary,
      });

      const sequenceResult = await processSequence({
        batch,
        sequenceNo,
        retryLimit: batch.retryLimit,
        startAttemptNo: sequencePlan.nextAttemptNo,
      });

      summary = withSummaryIncrements(
        {
          ...summary,
          running_sequence: null,
        },
        sequenceResult,
      );

      await updateSelfImprovementBatchSummary({
        batchId: batch.id,
        summary,
      });
    }

    const finalStatus: Extract<
      SelfImproveBatchStatus,
      "completed" | "completed_with_failures"
    > =
      asNumber(summary.final_failed_count) > 0 ? "completed_with_failures" : "completed";

    const finalized = await finalizeSelfImprovementBatch({
      batchId: batch.id,
      status: finalStatus,
      summary: {
        ...summary,
        running_sequence: null,
      },
    });

    return {
      status: "processed",
      batchId: batch.id,
      batchStatus: finalized.status,
      summary: finalized.summary,
    };
  } catch (error) {
    const failedSummary: SelfImprovementBatchSummary = {
      ...summary,
      running_sequence: null,
      worker_failure: {
        message: toErrorMessage(error),
      },
    };

    const finalized = await finalizeSelfImprovementBatch({
      batchId: batch.id,
      status: "failed",
      summary: failedSummary,
    });

    return {
      status: "processed",
      batchId: batch.id,
      batchStatus: finalized.status,
      summary: finalized.summary,
    };
  }
}

export { parseSelfImprovementPhrase };
export type { SelfImprovementPhraseIntent };

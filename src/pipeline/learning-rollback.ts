import type { AppliedChangeRecord, HarnessEvalResult } from "../types.js";
import {
  listRecentAppliedChanges,
  rollbackAppliedChangeTransactional,
} from "./persist.js";

function matchesWatchScope(
  change: AppliedChangeRecord,
  batchId: string | undefined,
  runId: string | undefined,
): boolean {
  const metadata = change.metadata ?? {};
  const changeBatchId = typeof metadata.batch_id === "string" ? metadata.batch_id : null;
  const changeRunId = typeof metadata.run_id === "string" ? metadata.run_id : null;

  if (batchId && changeBatchId === batchId) {
    return true;
  }
  if (runId && changeRunId === runId) {
    return true;
  }
  return !batchId && !runId;
}

export async function rollbackOnHarnessDegrade(input: {
  batchId?: string;
  runId?: string;
  harnessResult: HarnessEvalResult;
  watchLoops: number;
  rollbackOnDegrade: boolean;
}): Promise<{
  rolledBack: boolean;
  change: AppliedChangeRecord | null;
}> {
  if (!input.rollbackOnDegrade || input.harnessResult.passed) {
    return {
      rolledBack: false,
      change: null,
    };
  }

  const recentApplied = await listRecentAppliedChanges({
    status: "applied",
    limit: Math.max(1, input.watchLoops * 10),
  });

  const candidate = recentApplied.find((change) =>
    matchesWatchScope(change, input.batchId, input.runId),
  );
  if (!candidate) {
    return {
      rolledBack: false,
      change: null,
    };
  }

  const rolledBack = await rollbackAppliedChangeTransactional({
    appliedChangeId: candidate.id,
    reason: "harness_degrade_detected",
    metadata: {
      failed_metrics: input.harnessResult.failedMetrics,
      baseline_run_id: input.harnessResult.baselineRunId,
      candidate_run_id: input.harnessResult.candidateRunId,
    },
  });

  return {
    rolledBack: true,
    change: rolledBack,
  };
}

export async function rollbackByAppliedChangeId(input: {
  appliedChangeId: string;
  reason: string;
  metadata?: Record<string, unknown>;
}): Promise<AppliedChangeRecord> {
  return rollbackAppliedChangeTransactional({
    appliedChangeId: input.appliedChangeId,
    reason: input.reason,
    metadata: input.metadata,
  });
}

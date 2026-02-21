import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { getConfig } from "../config.js";
import {
  getSelfImprovementBatchDetails,
  listSelfImprovementBatches,
  type SelfImprovementBatchListItem,
  type SelfImprovementBatchDetails,
} from "../pipeline/persist.js";
import { parseSelfImprovementPhrase } from "../pipeline/self-improvement-orchestrator.js";
import { parseArgs } from "../utils/cli.js";

function parseLimit(value: string | boolean | undefined): number {
  if (typeof value !== "string") {
    return 20;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Invalid --limit value. Use a positive integer.");
  }
  return parsed;
}

function asNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function printJson(value: unknown): void {
  // eslint-disable-next-line no-console
  console.log(JSON.stringify(value, null, 2));
}

function deriveFallbackBatchStatusView(batch: SelfImprovementBatchListItem): Record<string, unknown> {
  return {
    batchId: batch.id,
    loopType: batch.loopType,
    status: batch.status,
    totalLoops: batch.requestedCount,
    completedLoops: asNumber(batch.summary.completed_loops),
    failedLoops: asNumber(batch.summary.final_failed_count),
    currentlyRunningLoop: batch.summary.running_sequence ?? null,
    lastFailureReason: null,
    retryAttempted: false,
    anyUpdatesAutoApplied: asNumber(batch.summary.auto_applied_updates_count) > 0,
    autoAppliedUpdatesCount: asNumber(batch.summary.auto_applied_updates_count),
    gatePassRate: asNumber(batch.summary.gate_pass_rate),
    summary: batch.summary,
    createdAt: batch.createdAt,
    startedAt: batch.startedAt,
    finishedAt: batch.finishedAt,
  };
}

function deriveDetailedBatchStatusView(batch: SelfImprovementBatchDetails): Record<string, unknown> {
  const bySequence = new Map<number, SelfImprovementBatchDetails["runs"][number]>();
  for (const run of batch.runs) {
    const previous = bySequence.get(run.sequenceNo);
    if (!previous || run.attemptNo > previous.attemptNo) {
      bySequence.set(run.sequenceNo, run);
    }
  }

  const latestPerSequence = [...bySequence.values()];
  const completedLoops = latestPerSequence.filter((run) => run.status !== "queued" && run.status !== "running").length;
  const failedLoops = latestPerSequence.filter(
    (run) => run.status === "failed" || run.status === "retried_failed",
  ).length;
  const currentlyRunning = latestPerSequence.find((run) => run.status === "running")?.sequenceNo ?? null;

  const failedAttempts = batch.runs
    .filter((run) => run.status === "failed" || run.status === "retried_failed")
    .sort((left, right) => {
      if (left.sequenceNo !== right.sequenceNo) {
        return right.sequenceNo - left.sequenceNo;
      }
      return right.attemptNo - left.attemptNo;
    });

  const lastFailedAttempt = failedAttempts[0] ?? null;
  const lastFailureReason =
    typeof lastFailedAttempt?.error?.message === "string"
      ? lastFailedAttempt.error.message
      : typeof lastFailedAttempt?.selfCorrectionContext?.failureSummary === "string"
        ? lastFailedAttempt.selfCorrectionContext.failureSummary
        : null;

  const retryAttempted = Boolean(lastFailedAttempt && lastFailedAttempt.attemptNo > 1);

  const autoAppliedUpdates = batch.runs.reduce((count, run) => {
    const runCount = asNumber(run.learningResult.auto_applied_updates);
    return count + runCount;
  }, 0);

  return {
    batchId: batch.id,
    loopType: batch.loopType,
    status: batch.status,
    totalLoops: batch.requestedCount,
    completedLoops,
    failedLoops,
    currentlyRunningLoop: currentlyRunning,
    lastFailureReason,
    retryAttempted,
    anyUpdatesAutoApplied: autoAppliedUpdates > 0,
    autoAppliedUpdatesCount: autoAppliedUpdates,
    gatePassRate: asNumber(batch.summary.gate_pass_rate),
    summary: batch.summary,
    createdAt: batch.createdAt,
    startedAt: batch.startedAt,
    finishedAt: batch.finishedAt,
  };
}

async function enrichBatchList(
  batches: SelfImprovementBatchListItem[],
): Promise<Record<string, unknown>[]> {
  return Promise.all(
    batches.map(async (batch) => {
      const details = await getSelfImprovementBatchDetails(batch.id);
      if (!details) {
        return deriveFallbackBatchStatusView(batch);
      }
      return deriveDetailedBatchStatusView(details);
    }),
  );
}

function buildSingleStatusOutput(batch: SelfImprovementBatchDetails | null): Record<string, unknown> {
  return {
    mode: "status_one",
    found: Boolean(batch),
    batch: batch ? deriveDetailedBatchStatusView(batch) : null,
    raw: batch,
  };
}

async function main(): Promise<void> {
  const config = getConfig();
  const args = parseArgs(process.argv.slice(2));
  const includeFinished = Boolean(args["include-finished"]);

  if (typeof args.phrase === "string" && args.phrase.trim().length > 0) {
    const intent = parseSelfImprovementPhrase(args.phrase, config.SELF_IMPROVE_MAX_LOOPS);
    await runMigrations();

    if (intent.kind === "status_all") {
      const batches = await listSelfImprovementBatches({
        limit: parseLimit(args.limit),
        includeFinished,
      });
      const enriched = await enrichBatchList(batches);
      printJson({
        mode: "status_all",
        count: enriched.length,
        batches: enriched,
      });
      return;
    }

    if (intent.kind === "status_one") {
      const batch = await getSelfImprovementBatchDetails(intent.batchId);
      printJson(buildSingleStatusOutput(batch));
      return;
    }

    throw new Error("The provided --phrase is not a status command.");
  }

  await runMigrations();

  if (typeof args["batch-id"] === "string" && args["batch-id"].trim().length > 0) {
    const batch = await getSelfImprovementBatchDetails(args["batch-id"].trim());
    printJson(buildSingleStatusOutput(batch));
    return;
  }

  const batches = await listSelfImprovementBatches({
    limit: parseLimit(args.limit),
    includeFinished,
  });
  const enriched = await enrichBatchList(batches);
  printJson({
    mode: "status_all",
    count: enriched.length,
    batches: enriched,
  });
}

main()
  .then(async () => {
    await closePool();
  })
  .catch(async (error: unknown) => {
    // eslint-disable-next-line no-console
    console.error("Self-improvement status failed:", error);
    await closePool();
    process.exitCode = 1;
  });

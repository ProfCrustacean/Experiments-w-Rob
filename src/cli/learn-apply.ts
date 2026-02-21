import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { getConfig } from "../config.js";
import { evaluateHarnessForRun } from "../pipeline/harness.js";
import { applyLearningProposals } from "../pipeline/learning-apply.js";
import { getPipelineRunById, recordHarnessRun } from "../pipeline/persist.js";
import { parseArgs } from "../utils/cli.js";

async function resolveStoreId(input: {
  requestedStoreId?: string;
  candidateRunId: string;
}): Promise<string> {
  if (input.requestedStoreId) {
    return input.requestedStoreId;
  }

  const run = await getPipelineRunById(input.candidateRunId);
  if (run) {
    return run.storeId;
  }

  const config = getConfig();
  if (!config.STORE_ID) {
    throw new Error("Missing store id. Provide --store or set STORE_ID.");
  }
  return config.STORE_ID;
}

async function main(): Promise<void> {
  const config = getConfig();
  const args = parseArgs(process.argv.slice(2));
  const batchId = typeof args["batch-id"] === "string" ? args["batch-id"].trim() : undefined;
  const runId = typeof args["run-id"] === "string" ? args["run-id"].trim() : undefined;
  const candidateRunId =
    typeof args["candidate-run-id"] === "string"
      ? args["candidate-run-id"].trim()
      : runId;

  if (!candidateRunId) {
    throw new Error("Missing candidate run id. Provide --candidate-run-id or --run-id.");
  }

  await runMigrations();

  const storeId = await resolveStoreId({
    requestedStoreId: typeof args.store === "string" ? args.store.trim() : undefined,
    candidateRunId,
  });

  const evaluation = await evaluateHarnessForRun({
    storeId,
    candidateRunId,
    baselineRunId:
      typeof args["baseline-run-id"] === "string" ? args["baseline-run-id"].trim() : undefined,
    benchmarkSnapshotId:
      typeof args["benchmark-snapshot-id"] === "string"
        ? args["benchmark-snapshot-id"].trim()
        : undefined,
  });

  const harnessRunId = await recordHarnessRun({
    batchId: batchId ?? null,
    runId: runId ?? candidateRunId,
    benchmarkSnapshotId: evaluation.benchmarkSnapshot.id,
    result: evaluation.result,
  });

  const applyResult = await applyLearningProposals({
    batchId,
    runId,
    harnessResult: evaluation.result,
    maxStructuralChangesPerLoop: config.SELF_IMPROVE_MAX_STRUCTURAL_CHANGES_PER_LOOP,
  });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        storeId,
        candidateRunId,
        harnessRunId,
        harnessResult: evaluation.result,
        benchmarkSnapshotId: evaluation.benchmarkSnapshot.id,
        applyResult,
      },
      null,
      2,
    ),
  );

  if (!evaluation.result.passed) {
    process.exitCode = 1;
  }
}

main()
  .then(async () => {
    await closePool();
  })
  .catch(async (error: unknown) => {
    // eslint-disable-next-line no-console
    console.error("Learn apply failed:", error);
    await closePool();
    process.exitCode = 1;
  });

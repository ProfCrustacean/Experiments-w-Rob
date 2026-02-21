import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { getConfig } from "../config.js";
import { evaluateHarnessForRun } from "../pipeline/harness.js";
import {
  getPipelineRunById,
  listPipelineRunsByStore,
  recordHarnessRun,
} from "../pipeline/persist.js";
import { parseArgs } from "../utils/cli.js";

async function resolveCandidateRunId(input: {
  requestedCandidateRunId?: string;
  storeId: string;
}): Promise<string> {
  if (input.requestedCandidateRunId) {
    return input.requestedCandidateRunId;
  }

  const recent = await listPipelineRunsByStore({
    storeId: input.storeId,
    limit: 20,
  });
  const latest = recent.find((run) => run.status !== "failed");
  if (!latest) {
    throw new Error(
      `Could not find a candidate run for store '${input.storeId}'. Provide --candidate-run-id explicitly.`,
    );
  }
  return latest.runId;
}

async function resolveStoreId(input: {
  argStoreId?: string;
  candidateRunId?: string;
}): Promise<string> {
  if (input.argStoreId) {
    return input.argStoreId;
  }

  if (input.candidateRunId) {
    const run = await getPipelineRunById(input.candidateRunId);
    if (!run) {
      throw new Error(`Candidate run ${input.candidateRunId} not found.`);
    }
    return run.storeId;
  }

  const config = getConfig();
  if (!config.STORE_ID) {
    throw new Error("Missing store id. Provide --store or set STORE_ID.");
  }
  return config.STORE_ID;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const requestedCandidateRunId =
    typeof args["candidate-run-id"] === "string" ? args["candidate-run-id"].trim() : undefined;
  const baselineRunId =
    typeof args["baseline-run-id"] === "string" ? args["baseline-run-id"].trim() : undefined;
  const benchmarkSnapshotId =
    typeof args["benchmark-snapshot-id"] === "string"
      ? args["benchmark-snapshot-id"].trim()
      : undefined;
  const batchId = typeof args["batch-id"] === "string" ? args["batch-id"].trim() : undefined;

  await runMigrations();

  const storeId = await resolveStoreId({
    argStoreId: typeof args.store === "string" ? args.store.trim() : undefined,
    candidateRunId: requestedCandidateRunId,
  });

  const candidateRunId = await resolveCandidateRunId({
    requestedCandidateRunId,
    storeId,
  });

  const evaluation = await evaluateHarnessForRun({
    storeId,
    candidateRunId,
    baselineRunId,
    benchmarkSnapshotId,
  });

  const harnessRunId = await recordHarnessRun({
    batchId: batchId ?? null,
    runId: candidateRunId,
    benchmarkSnapshotId: evaluation.benchmarkSnapshot.id,
    result: evaluation.result,
  });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        harnessRunId,
        storeId,
        candidateRunId,
        benchmarkSnapshot: evaluation.benchmarkSnapshot,
        result: evaluation.result,
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
    console.error("Harness eval failed:", error);
    await closePool();
    process.exitCode = 1;
  });

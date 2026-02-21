import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { getConfig } from "../config.js";
import { recoverStaleRunningRuns, recoverStaleSelfImprovementBatches } from "../pipeline/persist.js";
import { processNextSelfImprovementBatch } from "../pipeline/self-improvement-orchestrator.js";
import { parseArgs } from "../utils/cli.js";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parsePollMs(value: string | boolean | undefined, defaultMs: number): number {
  if (typeof value !== "string") {
    return defaultMs;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Invalid --poll-ms value. Use a positive integer.");
  }

  return parsed;
}

function parseStaleTimeoutMinutes(value: string | boolean | undefined, defaultMinutes: number): number {
  if (typeof value !== "string") {
    return defaultMinutes;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Invalid --stale-timeout-minutes value. Use a positive integer.");
  }

  return parsed;
}

async function main(): Promise<void> {
  const config = getConfig();
  const args = parseArgs(process.argv.slice(2));
  const once = Boolean(args.once);
  const pollMs = parsePollMs(args["poll-ms"], config.SELF_IMPROVE_WORKER_POLL_MS);
  const staleTimeoutMinutes = parseStaleTimeoutMinutes(
    args["stale-timeout-minutes"],
    config.SELF_IMPROVE_STALE_RUN_TIMEOUT_MINUTES,
  );

  await runMigrations();

  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (config.STORE_ID) {
      const recoveredPipelineRuns = await recoverStaleRunningRuns({
        storeId: config.STORE_ID,
        staleAfterMinutes: config.STALE_RUN_TIMEOUT_MINUTES,
      });
      if (recoveredPipelineRuns > 0) {
        // eslint-disable-next-line no-console
        console.log(
          JSON.stringify(
            {
              status: "recovered_stale_pipeline_runs",
              storeId: config.STORE_ID,
              staleTimeoutMinutes: config.STALE_RUN_TIMEOUT_MINUTES,
              recoveredRuns: recoveredPipelineRuns,
            },
            null,
            2,
          ),
        );
      }
    }

    const recovery = await recoverStaleSelfImprovementBatches({
      staleAfterMinutes: staleTimeoutMinutes,
    });

    if (recovery.recoveredRuns > 0 || recovery.requeuedBatches > 0) {
      // eslint-disable-next-line no-console
      console.log(
        JSON.stringify(
          {
            status: "recovered_stale_work",
            staleTimeoutMinutes,
            recoveredRuns: recovery.recoveredRuns,
            requeuedBatches: recovery.requeuedBatches,
          },
          null,
          2,
        ),
      );
    }

    const outcome = await processNextSelfImprovementBatch();
    if (outcome.status === "idle") {
      if (once) {
        // eslint-disable-next-line no-console
        console.log(
          JSON.stringify(
            {
              status: "idle",
              message: "No queued self-improvement batches.",
            },
            null,
            2,
          ),
        );
        return;
      }

      await sleep(pollMs);
      continue;
    }

    // eslint-disable-next-line no-console
    console.log(
      JSON.stringify(
        {
          status: "processed",
          batchId: outcome.batchId,
          batchStatus: outcome.batchStatus,
          summary: outcome.summary,
        },
        null,
        2,
      ),
    );

    if (once) {
      return;
    }
  }
}

main()
  .then(async () => {
    await closePool();
  })
  .catch(async (error: unknown) => {
    // eslint-disable-next-line no-console
    console.error("Self-improvement worker failed:", error);
    await closePool();
    process.exitCode = 1;
  });

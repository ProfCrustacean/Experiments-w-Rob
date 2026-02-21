import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { cancelSelfImprovementBatch } from "../pipeline/persist.js";
import { parseArgs, requireArg } from "../utils/cli.js";

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const batchId = requireArg(args, "batch-id");

  await runMigrations();
  const cancelled = await cancelSelfImprovementBatch(batchId);

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        batchId,
        cancelled: Boolean(cancelled),
        batch: cancelled,
      },
      null,
      2,
    ),
  );
}

main()
  .then(async () => {
    await closePool();
  })
  .catch(async (error: unknown) => {
    // eslint-disable-next-line no-console
    console.error("Self-improvement cancel failed:", error);
    await closePool();
    process.exitCode = 1;
  });

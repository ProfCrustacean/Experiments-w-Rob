import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { listPipelineRunsByStore, listRunArtifacts } from "../pipeline/persist.js";
import { parseArgs, requireArg } from "../utils/cli.js";

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

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const storeId = requireArg(args, "store");
  const limit = parseLimit(args.limit);

  await runMigrations();

  const runs = await listPipelineRunsByStore({ storeId, limit });
  const runsWithArtifacts = await Promise.all(
    runs.map(async (run) => {
      const artifacts = await listRunArtifacts({
        runId: run.runId,
        includeExpired: false,
      });

      return {
        runId: run.runId,
        runLabel: run.runLabel,
        status: run.status,
        startedAt: run.startedAt,
        finishedAt: run.finishedAt,
        artifactFormats: artifacts.map((artifact) => artifact.format),
        artifactCount: artifacts.length,
        artifacts,
      };
    }),
  );

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        storeId,
        limit,
        runCount: runsWithArtifacts.length,
        runs: runsWithArtifacts,
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
    console.error("Report list failed:", error);
    await closePool();
    process.exitCode = 1;
  });

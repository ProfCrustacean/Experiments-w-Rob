import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { rollbackByAppliedChangeId } from "../pipeline/learning-rollback.js";
import { listRecentAppliedChanges } from "../pipeline/persist.js";
import { parseArgs } from "../utils/cli.js";

async function resolveAppliedChangeId(
  provided: string | undefined,
  useLatest: boolean,
): Promise<string> {
  if (provided) {
    return provided;
  }

  if (!useLatest) {
    throw new Error("Missing --applied-change-id. Use --latest to rollback the most recent applied change.");
  }

  const recent = await listRecentAppliedChanges({
    status: "applied",
    limit: 1,
  });

  const latest = recent[0];
  if (!latest) {
    throw new Error("No applied changes available to rollback.");
  }

  return latest.id;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const providedId =
    typeof args["applied-change-id"] === "string" ? args["applied-change-id"].trim() : undefined;
  const useLatest = Boolean(args.latest);
  const reason =
    typeof args.reason === "string" && args.reason.trim().length > 0
      ? args.reason.trim()
      : "manual_cli_rollback";

  await runMigrations();

  const appliedChangeId = await resolveAppliedChangeId(providedId, useLatest);
  const rolledBack = await rollbackByAppliedChangeId({
    appliedChangeId,
    reason,
    metadata: {
      invoked_by: "learn:rollback",
    },
  });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        appliedChangeId,
        rolledBack,
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
    console.error("Learn rollback failed:", error);
    await closePool();
    process.exitCode = 1;
  });

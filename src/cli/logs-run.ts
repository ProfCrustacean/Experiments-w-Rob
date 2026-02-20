import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { listRunLogs } from "../pipeline/persist.js";
import { parseArgs, requireArg } from "../utils/cli.js";

function parsePositiveInt(raw: string | boolean | undefined, fallback: number): number {
  if (typeof raw !== "string") {
    return fallback;
  }

  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error("Invalid numeric argument. Expected a positive integer.");
  }
  return value;
}

function parseLevel(raw: string | boolean | undefined): "debug" | "info" | "warn" | "error" | undefined {
  if (typeof raw !== "string") {
    return undefined;
  }

  if (raw === "debug" || raw === "info" || raw === "warn" || raw === "error") {
    return raw;
  }

  throw new Error("Invalid --level value. Use debug, info, warn, or error.");
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const runId = requireArg(args, "run-id");
  const limit = parsePositiveInt(args.limit, 500);
  const includeExpired = Boolean(args["include-expired"]);
  const stage = typeof args.stage === "string" ? args.stage : undefined;
  const event = typeof args.event === "string" ? args.event : undefined;
  const level = parseLevel(args.level);

  await runMigrations();

  const rows = await listRunLogs({
    runId,
    limit,
    includeExpired,
    stage,
    event,
    level,
  });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        runId,
        count: rows.length,
        limit,
        includeExpired,
        stage,
        event,
        level,
        rows,
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
    console.error("Run logs query failed:", error);
    await closePool();
    process.exitCode = 1;
  });

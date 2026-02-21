import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { getConfig } from "../config.js";
import { updateQualityScoreboard } from "../docs/quality-scoreboard.js";
import { parseArgs } from "../utils/cli.js";

function parseWindowDays(value: string | boolean | undefined): number {
  if (typeof value !== "string") {
    return 7;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Invalid --window-days value. Use a positive integer.");
  }

  return parsed;
}

async function main(): Promise<void> {
  const config = getConfig();
  const args = parseArgs(process.argv.slice(2));

  const outputPath =
    typeof args.output === "string" && args.output.trim().length > 0
      ? args.output.trim()
      : "docs/quality-scoreboard.md";
  const windowDays = parseWindowDays(args["window-days"]);
  const storeId = typeof args.store === "string" ? args.store.trim() : config.STORE_ID ?? null;

  await runMigrations();
  const result = await updateQualityScoreboard({
    outputPath,
    windowDays,
    storeId,
  });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        outputPath: result.outputPath,
        updatedAt: result.updatedAt,
        windowDays: result.windowDays,
        storeScope: result.storeScope,
        docsErrors: result.docsErrors,
        docsWarnings: result.docsWarnings,
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
    console.error("Docs scoreboard update failed:", error);
    await closePool();
    process.exitCode = 1;
  });

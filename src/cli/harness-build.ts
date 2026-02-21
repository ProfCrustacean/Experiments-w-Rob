import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { getConfig } from "../config.js";
import { buildHarnessBenchmarkSnapshot } from "../pipeline/harness.js";
import { parseArgs } from "../utils/cli.js";

function resolveStoreId(argStore: string | boolean | undefined): string {
  if (typeof argStore === "string" && argStore.trim().length > 0) {
    return argStore.trim();
  }

  const config = getConfig();
  if (!config.STORE_ID) {
    throw new Error("Missing store id. Provide --store or set STORE_ID.");
  }
  return config.STORE_ID;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const storeId = resolveStoreId(args.store);

  await runMigrations();
  const snapshot = await buildHarnessBenchmarkSnapshot({ storeId });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        storeId,
        snapshot,
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
    console.error("Harness build failed:", error);
    await closePool();
    process.exitCode = 1;
  });

import path from "node:path";
import { closePool } from "../db/client.js";
import { runPipeline } from "../pipeline/run.js";

async function main(): Promise<void> {
  const inputPathEnv = process.env.CATALOG_INPUT_PATH;
  const storeId = process.env.STORE_ID;
  const runLabel = process.env.RUN_LABEL;

  if (!inputPathEnv) {
    throw new Error("Missing CATALOG_INPUT_PATH environment variable.");
  }
  if (!storeId) {
    throw new Error("Missing STORE_ID environment variable.");
  }

  const summary = await runPipeline({
    inputPath: path.resolve(process.cwd(), inputPathEnv),
    storeId,
    runLabel,
  });

  // eslint-disable-next-line no-console
  console.log(JSON.stringify(summary, null, 2));
}

main()
  .then(async () => {
    await closePool();
  })
  .catch(async (error: unknown) => {
    // eslint-disable-next-line no-console
    console.error("Pipeline-from-env failed:", error);
    await closePool();
    process.exitCode = 1;
  });

import path from "node:path";
import { closePool } from "../db/client.js";
import { runPipeline } from "../pipeline/run.js";
import { parseArgs, requireArg } from "../utils/cli.js";

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const inputPath = requireArg(args, "input");
  const storeId = requireArg(args, "store");
  const runLabel = typeof args["run-label"] === "string" ? args["run-label"] : undefined;
  const sampleParts =
    typeof args["sample-parts"] === "string" ? Number(args["sample-parts"]) : undefined;
  const samplePartIndex =
    typeof args["sample-part-index"] === "string"
      ? Number(args["sample-part-index"])
      : undefined;

  const summary = await runPipeline({
    inputPath: path.resolve(process.cwd(), inputPath),
    storeId,
    runLabel,
    sampleParts,
    samplePartIndex,
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
    console.error("Pipeline failed:", error);
    await closePool();
    process.exitCode = 1;
  });

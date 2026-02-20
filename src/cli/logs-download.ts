import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { getConfig } from "../config.js";
import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { exportRunLogs } from "../pipeline/persist.js";
import { parseArgs, requireArg } from "../utils/cli.js";

async function main(): Promise<void> {
  const config = getConfig();
  const args = parseArgs(process.argv.slice(2));
  const runId = requireArg(args, "run-id");
  const includeExpired = Boolean(args["include-expired"]);
  const outDirArg = typeof args.out === "string" ? args.out : config.OUTPUT_DIR;
  const outDir = path.resolve(process.cwd(), outDirArg);

  await runMigrations();

  const rows = await exportRunLogs({
    runId,
    includeExpired,
  });

  await mkdir(outDir, { recursive: true });
  const outputPath = path.join(outDir, `run_logs_${runId}.jsonl`);
  const content = rows.map((row) => JSON.stringify(row)).join("\n");
  await writeFile(outputPath, content.length > 0 ? `${content}\n` : "", "utf8");

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        runId,
        count: rows.length,
        includeExpired,
        outputPath,
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
    console.error("Run logs download failed:", error);
    await closePool();
    process.exitCode = 1;
  });

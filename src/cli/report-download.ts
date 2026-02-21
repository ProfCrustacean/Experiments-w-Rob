import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { getConfig } from "../config.js";
import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { getRunArtifact } from "../pipeline/persist.js";
import { FORMAT_TO_ARTIFACT_KEY } from "../pipeline/run-artifacts.js";
import type { RunArtifactFormat } from "../types.js";
import { parseArgs, requireArg } from "../utils/cli.js";

function parseFormat(value: string): RunArtifactFormat {
  if (value === "xlsx" || value === "csv" || value === "qa-csv" || value === "confusion-csv") {
    return value;
  }
  throw new Error("Invalid --format value. Use xlsx, csv, qa-csv, or confusion-csv.");
}

async function main(): Promise<void> {
  const config = getConfig();
  const args = parseArgs(process.argv.slice(2));
  const runId = requireArg(args, "run-id");
  const formatArg = requireArg(args, "format");
  const format = parseFormat(formatArg);
  const outDirArg = typeof args.out === "string" ? args.out : config.OUTPUT_DIR;
  const outDir = path.resolve(process.cwd(), outDirArg);

  await runMigrations();

  const result = await getRunArtifact({
    runId,
    artifactKey: FORMAT_TO_ARTIFACT_KEY[format],
  });

  if (result.status === "missing") {
    throw new Error(`No artifact found for run ${runId} and format ${format}.`);
  }

  if (result.status === "expired") {
    throw new Error(
      `Artifact for run ${runId} and format ${format} expired at ${result.expiresAt}.`,
    );
  }

  await mkdir(outDir, { recursive: true });
  const outputPath = path.join(outDir, result.artifact.fileName);
  await writeFile(outputPath, result.artifact.content);

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        runId,
        format,
        outputPath,
        fileName: result.artifact.fileName,
        sizeBytes: result.artifact.sizeBytes,
        expiresAt: result.artifact.expiresAt,
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
    console.error("Report download failed:", error);
    await closePool();
    process.exitCode = 1;
  });

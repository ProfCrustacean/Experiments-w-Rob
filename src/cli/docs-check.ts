import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { parseArgs } from "../utils/cli.js";
import { runDocsChecks, toMarkdownReport } from "../docs/health-check.js";

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const reportPath =
    typeof args["report-path"] === "string" && args["report-path"].trim().length > 0
      ? args["report-path"].trim()
      : null;
  const jsonMode = Boolean(args.json);

  const summary = await runDocsChecks();

  if (reportPath) {
    const absoluteReportPath = path.resolve(process.cwd(), reportPath);
    await mkdir(path.dirname(absoluteReportPath), {
      recursive: true,
    });
    await writeFile(absoluteReportPath, toMarkdownReport(summary), "utf8");
  }

  if (jsonMode) {
    // eslint-disable-next-line no-console
    console.log(JSON.stringify(summary, null, 2));
  } else {
    // eslint-disable-next-line no-console
    console.log(`Docs check complete: ${summary.errors} errors, ${summary.warnings} warnings.`);
    for (const finding of summary.findings) {
      // eslint-disable-next-line no-console
      console.log(`[${finding.severity.toUpperCase()}] ${finding.code}: ${finding.message}`);
    }
  }

  if (summary.errors > 0) {
    process.exitCode = 1;
  }
}

main().catch((error: unknown) => {
  // eslint-disable-next-line no-console
  console.error("Docs check failed:", error);
  process.exitCode = 1;
});

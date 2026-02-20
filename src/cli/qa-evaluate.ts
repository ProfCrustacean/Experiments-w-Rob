import path from "node:path";
import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { updateRunStatusFromQAEvaluation } from "../pipeline/persist.js";
import { evaluateQAReport } from "../pipeline/qa-report.js";
import { parseArgs, requireArg } from "../utils/cli.js";

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const file = requireArg(args, "file");
  const thresholdRaw = typeof args.threshold === "string" ? Number(args.threshold) : 0.85;
  const threshold = Number.isFinite(thresholdRaw) ? thresholdRaw : 0.85;
  const noDb = Boolean(args["no-db"]);

  const result = await evaluateQAReport(path.resolve(process.cwd(), file));

  if (!noDb) {
    await runMigrations();
    await updateRunStatusFromQAEvaluation({
      runId: result.runId,
      passRate: result.passRate,
      threshold,
      stats: {
        qa_total_rows: result.totalRows,
        qa_reviewed_rows: result.reviewedRows,
        qa_pass_rows: result.passRows,
        qa_fail_rows: result.failRows,
      },
    });
  }

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        ...result,
        threshold,
        status: result.passRate >= threshold ? "accepted" : "rejected",
        dbUpdated: !noDb,
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
    console.error("QA evaluation failed:", error);
    await closePool();
    process.exitCode = 1;
  });

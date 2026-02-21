import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { generateLearningProposals } from "../pipeline/learning-proposal-generator.js";
import { getPipelineRunById } from "../pipeline/persist.js";
import { parseArgs } from "../utils/cli.js";

function parseMetrics(value: string | boolean | undefined): string[] {
  if (typeof value !== "string" || value.trim().length === 0) {
    return [];
  }
  return value
    .split(",")
    .map((metric) => metric.trim())
    .filter((metric) => metric.length > 0);
}

function parseAlerts(value: unknown): Array<{
  category_slug: string;
  affected_count: number;
  low_margin_count: number;
  contradiction_count: number;
  fallback_count: number;
}> {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .filter((entry) => typeof entry === "object" && entry !== null)
    .map((entry) => entry as Record<string, unknown>)
    .map((entry) => ({
      category_slug: String(entry.category_slug ?? ""),
      affected_count: Number(entry.affected_count ?? 0),
      low_margin_count: Number(entry.low_margin_count ?? 0),
      contradiction_count: Number(entry.contradiction_count ?? 0),
      fallback_count: Number(entry.fallback_count ?? 0),
    }))
    .filter((entry) => entry.category_slug.length > 0);
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const batchId = typeof args["batch-id"] === "string" ? args["batch-id"].trim() : undefined;
  const runId = typeof args["run-id"] === "string" ? args["run-id"].trim() : undefined;

  await runMigrations();

  const failedGateMetrics = parseMetrics(args["failed-metrics"]);
  let topConfusionAlerts: ReturnType<typeof parseAlerts> = [];

  if (runId) {
    const runRecord = await getPipelineRunById(runId);
    if (!runRecord) {
      throw new Error(`Run ${runId} not found.`);
    }
    topConfusionAlerts = parseAlerts(runRecord.stats.top_confusion_alerts);
  }

  const outcome = await generateLearningProposals({
    batchId,
    runId,
    failedGateMetrics,
    topConfusionAlerts,
  });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        batchId: batchId ?? null,
        runId: runId ?? null,
        failedGateMetrics,
        proposalsGenerated: outcome.proposals.length,
        proposalIds: outcome.proposals.map((proposal) => proposal.id),
        summaryPatch: outcome.summaryPatch,
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
    console.error("Learn propose failed:", error);
    await closePool();
    process.exitCode = 1;
  });

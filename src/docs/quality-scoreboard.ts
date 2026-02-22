import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { getPool } from "../db/client.js";
import { getConfig } from "../config.js";
import { countFindingsByCode, runDocsChecks, type DocsCheckSummary } from "./health-check.js";

interface TimeWindow {
  start: Date;
  end: Date;
}

interface ScoreboardOptions {
  outputPath?: string;
  windowDays?: number;
  storeId?: string | null;
}

interface ScoreboardResult {
  outputPath: string;
  updatedAt: string;
  windowDays: number;
  storeScope: string;
  docsErrors: number;
  docsWarnings: number;
  agentDocCount: number;
  oversizeDocCount: number;
  missingSectionCount: number;
  moduleCoverageRate: number;
  ownerCoverageRate: number;
}

interface PipelineRunRow {
  run_label: string | null;
  stats_json: Record<string, unknown>;
}

interface BatchSummaryRow {
  summary_jsonb: Record<string, unknown>;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function average(values: number[]): number | null {
  if (values.length === 0) {
    return null;
  }
  const total = values.reduce((sum, value) => sum + value, 0);
  return total / values.length;
}

function sum(values: number[]): number {
  return values.reduce((total, value) => total + value, 0);
}

function formatRate(value: number | null): string {
  if (value === null) {
    return "n/a";
  }
  return value.toFixed(4);
}

function formatCount(value: number | null): string {
  if (value === null) {
    return "n/a";
  }
  return String(Math.round(value));
}

function trend(
  current: number | null,
  previous: number | null,
  direction: "up_good" | "down_good",
  tolerance: number,
): string {
  if (current === null || previous === null) {
    return "n/a";
  }

  const delta = current - previous;
  if (Math.abs(delta) <= tolerance) {
    return "stable";
  }

  if (direction === "up_good") {
    return delta > 0 ? "up" : "down";
  }

  return delta < 0 ? "up" : "down";
}

function buildTimeWindow(windowDays: number, offsetWindows: number): TimeWindow {
  const now = Date.now();
  const windowMs = windowDays * 24 * 60 * 60 * 1000;
  const end = new Date(now - offsetWindows * windowMs);
  const start = new Date(end.getTime() - windowMs);
  return {
    start,
    end,
  };
}

async function queryPipelineRuns(window: TimeWindow, storeId: string | null): Promise<PipelineRunRow[]> {
  const pool = getPool();
  const result = await pool.query<PipelineRunRow>(
    `
      SELECT run_label, stats_json
      FROM pipeline_runs
      WHERE status IN ('completed_pending_review', 'accepted', 'rejected')
        AND started_at >= $1
        AND started_at < $2
        AND ($3::text IS NULL OR store_id = $3)
    `,
    [window.start.toISOString(), window.end.toISOString(), storeId],
  );

  return result.rows;
}

async function queryBatchSummaries(window: TimeWindow): Promise<BatchSummaryRow[]> {
  const pool = getPool();
  const result = await pool.query<BatchSummaryRow>(
    `
      SELECT summary_jsonb
      FROM self_improvement_batches
      WHERE status IN ('completed', 'completed_with_failures')
        AND created_at >= $1
        AND created_at < $2
    `,
    [window.start.toISOString(), window.end.toISOString()],
  );

  return result.rows;
}

async function queryRollbackCount(window: TimeWindow, storeId: string | null): Promise<number> {
  const pool = getPool();
  const result = await pool.query<{ rollback_count: number }>(
    `
      SELECT COUNT(*)::int AS rollback_count
      FROM self_improvement_rollback_events AS events
      LEFT JOIN self_improvement_applied_changes AS applied
        ON applied.id = events.applied_change_id
      LEFT JOIN self_improvement_proposals AS proposals
        ON proposals.id = applied.proposal_id
      LEFT JOIN pipeline_runs AS runs
        ON runs.id = proposals.run_id
      WHERE events.created_at >= $1
        AND events.created_at < $2
        AND ($3::text IS NULL OR runs.store_id = $3)
    `,
    [window.start.toISOString(), window.end.toISOString(), storeId],
  );

  return Number(result.rows[0]?.rollback_count ?? 0);
}

function collectRunRates(rows: PipelineRunRow[], key: string): number[] {
  const values: number[] = [];
  for (const row of rows) {
    const value = asNumber(row.stats_json?.[key]);
    if (value !== null) {
      values.push(value);
    }
  }
  return values;
}

function collectCanaryAutoAcceptedRates(rows: PipelineRunRow[]): number[] {
  const values: number[] = [];
  for (const row of rows) {
    if (!row.run_label?.startsWith("canary-")) {
      continue;
    }
    const value = asNumber(row.stats_json?.auto_accepted_rate);
    if (value !== null) {
      values.push(value);
    }
  }
  return values;
}

function collectBatchMetric(rows: BatchSummaryRow[], key: string): number[] {
  const values: number[] = [];
  for (const row of rows) {
    const value = asNumber(row.summary_jsonb?.[key]);
    if (value !== null) {
      values.push(value);
    }
  }
  return values;
}

function buildDebtQueue(input: {
  fallbackRate: number | null;
  needsReviewRate: number | null;
  canaryAutoAcceptedRate: number | null;
  gatePassRate: number | null;
  docsSummary: DocsCheckSummary;
  rollbackCount: number;
  config: ReturnType<typeof getConfig>;
}): string[] {
  const debt: string[] = [];

  if (
    input.fallbackRate !== null &&
    input.fallbackRate > input.config.HARNESS_MAX_FALLBACK_RATE
  ) {
    debt.push(
      `Fallback rate is above target (${formatRate(input.fallbackRate)} > ${input.config.HARNESS_MAX_FALLBACK_RATE.toFixed(4)}).`,
    );
  }

  if (
    input.needsReviewRate !== null &&
    input.needsReviewRate > input.config.HARNESS_MAX_NEEDS_REVIEW_RATE
  ) {
    debt.push(
      `Needs-review rate is above target (${formatRate(input.needsReviewRate)} > ${input.config.HARNESS_MAX_NEEDS_REVIEW_RATE.toFixed(4)}).`,
    );
  }

  if (
    input.canaryAutoAcceptedRate !== null &&
    input.canaryAutoAcceptedRate < input.config.CANARY_AUTO_ACCEPT_THRESHOLD
  ) {
    debt.push(
      `Canary auto-accepted rate is below threshold (${formatRate(input.canaryAutoAcceptedRate)} < ${input.config.CANARY_AUTO_ACCEPT_THRESHOLD.toFixed(4)}).`,
    );
  }

  if (input.gatePassRate !== null && input.gatePassRate < 0.7) {
    debt.push(`Batch gate pass rate is low (${formatRate(input.gatePassRate)}).`);
  }

  if (input.rollbackCount > 0) {
    debt.push(`Rollback events detected in current window (${input.rollbackCount}).`);
  }

  if (input.docsSummary.errors > 0) {
    debt.push(`Documentation health has ${input.docsSummary.errors} blocking error(s).`);
  }
  if (input.docsSummary.warnings > 0) {
    debt.push(`Documentation health has ${input.docsSummary.warnings} warning(s) to clean up.`);
  }
  if (input.docsSummary.oversizeDocCount > 0) {
    debt.push(`Oversized docs detected (${input.docsSummary.oversizeDocCount}).`);
  }
  if (input.docsSummary.missingSectionCount > 0) {
    debt.push(
      `Task/module cards are missing required sections (${input.docsSummary.missingSectionCount}).`,
    );
  }
  if (input.docsSummary.moduleCoverageRate < 1) {
    debt.push(
      `Module card coverage is incomplete (${(input.docsSummary.moduleCoverageRate * 100).toFixed(1)}%).`,
    );
  }
  if (input.docsSummary.ownerCoverageRate < 1) {
    debt.push(
      `Owner mapping coverage is incomplete (${(input.docsSummary.ownerCoverageRate * 100).toFixed(1)}%).`,
    );
  }

  if (debt.length === 0) {
    debt.push("No active debt items.");
  }

  return debt;
}

async function readPreviousScoreboard(pathname: string): Promise<string | null> {
  try {
    return await readFile(pathname, "utf8");
  } catch {
    return null;
  }
}

function buildScoreboardMarkdown(input: {
  updatedAt: string;
  windowDays: number;
  storeScope: string;
  runCountCurrent: number;
  runCountPrevious: number;
  batchCountCurrent: number;
  batchCountPrevious: number;
  fallbackCurrent: number | null;
  fallbackPrevious: number | null;
  needsReviewCurrent: number | null;
  needsReviewPrevious: number | null;
  canaryAutoAcceptedCurrent: number | null;
  canaryAutoAcceptedPrevious: number | null;
  gatePassRateCurrent: number | null;
  gatePassRatePrevious: number | null;
  autoAppliedUpdatesCurrent: number;
  autoAppliedUpdatesPrevious: number;
  rollbackCurrent: number;
  rollbackPrevious: number;
  docsSummary: DocsCheckSummary;
  docsCountsByCode: Record<string, number>;
  debtQueue: string[];
  config: ReturnType<typeof getConfig>;
}): string {
  const docsUnknownScripts = input.docsCountsByCode.UNKNOWN_SCRIPT_REF ?? 0;
  const docsBrokenPaths = input.docsCountsByCode.BROKEN_PATH_REF ?? 0;

  const lines: string[] = [];
  lines.push("# Quality Scoreboard");
  lines.push("");
  lines.push(`- Last updated: ${input.updatedAt}`);
  lines.push(`- Window: trailing ${input.windowDays} days`);
  lines.push(`- Store scope: ${input.storeScope}`);
  lines.push(`- Pipeline runs in scope: ${input.runCountCurrent} (previous window: ${input.runCountPrevious})`);
  lines.push(`- Self-improvement batches in scope: ${input.batchCountCurrent} (previous window: ${input.batchCountPrevious})`);
  lines.push("");
  lines.push("## Core metrics");
  lines.push("");
  lines.push("| Metric | Target | Current | Trend | Notes |");
  lines.push("| --- | --- | --- | --- | --- |");
  lines.push(
    `| \`fallback_category_rate\` | <= ${input.config.HARNESS_MAX_FALLBACK_RATE.toFixed(4)} | ${formatRate(input.fallbackCurrent)} | ${trend(input.fallbackCurrent, input.fallbackPrevious, "down_good", 0.0005)} | From pipeline run stats |`,
  );
  lines.push(
    `| \`needs_review_rate\` | <= ${input.config.HARNESS_MAX_NEEDS_REVIEW_RATE.toFixed(4)} | ${formatRate(input.needsReviewCurrent)} | ${trend(input.needsReviewCurrent, input.needsReviewPrevious, "down_good", 0.0005)} | From pipeline run stats |`,
  );
  lines.push(
    `| \`auto_accepted_rate\` | >= ${input.config.CANARY_AUTO_ACCEPT_THRESHOLD.toFixed(4)} (canary) | ${formatRate(input.canaryAutoAcceptedCurrent)} | ${trend(input.canaryAutoAcceptedCurrent, input.canaryAutoAcceptedPrevious, "up_good", 0.0005)} | Canary-labeled runs only |`,
  );
  lines.push(
    `| \`gate_pass_rate\` | increasing | ${formatRate(input.gatePassRateCurrent)} | ${trend(input.gatePassRateCurrent, input.gatePassRatePrevious, "up_good", 0.005)} | From self-improvement batch summaries |`,
  );
  lines.push(
    `| \`auto_applied_updates_count\` | stable growth | ${formatCount(input.autoAppliedUpdatesCurrent)} | ${trend(input.autoAppliedUpdatesCurrent, input.autoAppliedUpdatesPrevious, "up_good", 0)} | Sum across batch summaries |`,
  );
  lines.push(
    `| \`rollbacks_triggered\` | low/stable | ${formatCount(input.rollbackCurrent)} | ${trend(input.rollbackCurrent, input.rollbackPrevious, "down_good", 0)} | From rollback events |`,
  );
  lines.push("");
  lines.push("## Documentation health");
  lines.push("");
  lines.push("| Metric | Target | Current | Trend | Notes |");
  lines.push("| --- | --- | --- | --- | --- |");
  lines.push(
    `| \`docs:check\` errors | 0 | ${input.docsSummary.errors} | ${trend(input.docsSummary.errors, null, "down_good", 0)} | Blocking doc correctness issues |`,
  );
  lines.push(
    `| \`docs:check\` warnings | downward trend | ${input.docsSummary.warnings} | ${trend(input.docsSummary.warnings, null, "down_good", 0)} | Non-blocking cleanup items |`,
  );
  lines.push(
    `| Broken doc path refs | 0 | ${docsBrokenPaths} | ${trend(docsBrokenPaths, null, "down_good", 0)} | \`BROKEN_PATH_REF\` findings |`,
  );
  lines.push(
    `| Unknown documented scripts | 0 | ${docsUnknownScripts} | ${trend(docsUnknownScripts, null, "down_good", 0)} | \`UNKNOWN_SCRIPT_REF\` findings |`,
  );
  lines.push(
    `| Agent docs tracked | increasing | ${formatCount(input.docsSummary.agentDocCount)} | n/a | Markdown files under \`docs/agents/\` |`,
  );
  lines.push(
    `| Oversize docs | 0 | ${formatCount(input.docsSummary.oversizeDocCount)} | n/a | \`DOC_TOO_LARGE\` findings |`,
  );
  lines.push(
    `| Missing card sections | 0 | ${formatCount(input.docsSummary.missingSectionCount)} | n/a | \`MISSING_REQUIRED_SECTION\` findings |`,
  );
  lines.push(
    `| Module card coverage | 1.0000 | ${formatRate(input.docsSummary.moduleCoverageRate)} | n/a | Required module cards present |`,
  );
  lines.push(
    `| Owner coverage | 1.0000 | ${formatRate(input.docsSummary.ownerCoverageRate)} | n/a | Owner key mapped to ownership map |`,
  );
  lines.push("");
  lines.push("## Open debt queue");
  lines.push("");
  lines.push("- Keep this list short and specific.");
  lines.push("- Move completed items into the decisions log if they change policy.");
  lines.push("");

  input.debtQueue.forEach((item, index) => {
    lines.push(`${index + 1}. ${item}`);
  });

  lines.push("");
  return `${lines.join("\n")}\n`;
}

export async function updateQualityScoreboard(options?: ScoreboardOptions): Promise<ScoreboardResult> {
  const config = getConfig();
  const windowDays = Math.max(1, Math.floor(options?.windowDays ?? 7));
  const storeId = options?.storeId ?? config.STORE_ID ?? null;
  const outputPath = path.resolve(process.cwd(), options?.outputPath ?? "docs/quality-scoreboard.md");

  const currentWindow = buildTimeWindow(windowDays, 0);
  const previousWindow = buildTimeWindow(windowDays, 1);

  const [currentRuns, previousRuns, currentBatches, previousBatches, rollbackCurrent, rollbackPrevious] =
    await Promise.all([
      queryPipelineRuns(currentWindow, storeId),
      queryPipelineRuns(previousWindow, storeId),
      queryBatchSummaries(currentWindow),
      queryBatchSummaries(previousWindow),
      queryRollbackCount(currentWindow, storeId),
      queryRollbackCount(previousWindow, storeId),
    ]);

  const fallbackCurrent = average(collectRunRates(currentRuns, "fallback_category_rate"));
  const fallbackPrevious = average(collectRunRates(previousRuns, "fallback_category_rate"));
  const needsReviewCurrent = average(collectRunRates(currentRuns, "needs_review_rate"));
  const needsReviewPrevious = average(collectRunRates(previousRuns, "needs_review_rate"));
  const canaryAutoAcceptedCurrent = average(collectCanaryAutoAcceptedRates(currentRuns));
  const canaryAutoAcceptedPrevious = average(collectCanaryAutoAcceptedRates(previousRuns));
  const gatePassRateCurrent = average(collectBatchMetric(currentBatches, "gate_pass_rate"));
  const gatePassRatePrevious = average(collectBatchMetric(previousBatches, "gate_pass_rate"));
  const autoAppliedUpdatesCurrent = sum(collectBatchMetric(currentBatches, "auto_applied_updates_count"));
  const autoAppliedUpdatesPrevious = sum(collectBatchMetric(previousBatches, "auto_applied_updates_count"));

  const docsSummary = await runDocsChecks({
    skipScoreboardFreshness: true,
  });
  const docsCountsByCode = countFindingsByCode(docsSummary);

  const debtQueue = buildDebtQueue({
    fallbackRate: fallbackCurrent,
    needsReviewRate: needsReviewCurrent,
    canaryAutoAcceptedRate: canaryAutoAcceptedCurrent,
    gatePassRate: gatePassRateCurrent,
    docsSummary,
    rollbackCount: rollbackCurrent,
    config,
  });

  const updatedAt = new Date().toISOString();
  const markdown = buildScoreboardMarkdown({
    updatedAt,
    windowDays,
    storeScope: storeId ?? "all stores",
    runCountCurrent: currentRuns.length,
    runCountPrevious: previousRuns.length,
    batchCountCurrent: currentBatches.length,
    batchCountPrevious: previousBatches.length,
    fallbackCurrent,
    fallbackPrevious,
    needsReviewCurrent,
    needsReviewPrevious,
    canaryAutoAcceptedCurrent,
    canaryAutoAcceptedPrevious,
    gatePassRateCurrent,
    gatePassRatePrevious,
    autoAppliedUpdatesCurrent,
    autoAppliedUpdatesPrevious,
    rollbackCurrent,
    rollbackPrevious,
    docsSummary,
    docsCountsByCode,
    debtQueue,
    config,
  });

  const previous = await readPreviousScoreboard(outputPath);
  if (previous !== markdown) {
    await writeFile(outputPath, markdown, "utf8");
  }

  return {
    outputPath,
    updatedAt,
    windowDays,
    storeScope: storeId ?? "all stores",
    docsErrors: docsSummary.errors,
    docsWarnings: docsSummary.warnings,
    agentDocCount: docsSummary.agentDocCount,
    oversizeDocCount: docsSummary.oversizeDocCount,
    missingSectionCount: docsSummary.missingSectionCount,
    moduleCoverageRate: docsSummary.moduleCoverageRate,
    ownerCoverageRate: docsSummary.ownerCoverageRate,
  };
}

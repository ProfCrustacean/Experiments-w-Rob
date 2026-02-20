import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { parse } from "csv-parse/sync";
import { sampleWithoutReplacement } from "../utils/collections.js";
import { safeJsonString } from "../utils/text.js";

export interface QAReportRow {
  run_id: string;
  source_sku: string;
  title: string;
  predicted_category: string;
  needs_review: boolean;
  key_attributes: string;
  review_status: string;
  review_notes: string;
}

function escapeCsv(value: string): string {
  if (/[",\n]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function stringifyCsv(rows: QAReportRow[]): string {
  const header = [
    "run_id",
    "source_sku",
    "title",
    "predicted_category",
    "needs_review",
    "key_attributes",
    "review_status",
    "review_notes",
  ];

  const body = rows.map((row) =>
    [
      row.run_id,
      row.source_sku,
      row.title,
      row.predicted_category,
      String(row.needs_review),
      row.key_attributes,
      row.review_status,
      row.review_notes,
    ]
      .map(escapeCsv)
      .join(","),
  );

  return [header.join(","), ...body].join("\n");
}

export async function writeQAReport(input: {
  outputDir: string;
  runId: string;
  rows: Array<{
    sourceSku: string;
    title: string;
    predictedCategory: string;
    needsReview: boolean;
    attributeValues: Record<string, unknown>;
  }>;
  sampleSize: number;
}): Promise<{ filePath: string; sampledRows: number; totalRows: number }> {
  await mkdir(input.outputDir, { recursive: true });

  const sampled = sampleWithoutReplacement(input.rows, input.sampleSize);
  const reportRows: QAReportRow[] = sampled.map((row) => ({
    run_id: input.runId,
    source_sku: row.sourceSku,
    title: row.title,
    predicted_category: row.predictedCategory,
    needs_review: row.needsReview,
    key_attributes: safeJsonString(row.attributeValues),
    review_status: "",
    review_notes: "",
  }));

  const fileName = `qa_report_${input.runId}.csv`;
  const filePath = path.join(input.outputDir, fileName);
  await writeFile(filePath, stringifyCsv(reportRows), "utf8");

  return {
    filePath,
    sampledRows: reportRows.length,
    totalRows: input.rows.length,
  };
}

export async function evaluateQAReport(filePath: string): Promise<{
  runId: string;
  totalRows: number;
  reviewedRows: number;
  passRows: number;
  failRows: number;
  passRate: number;
}> {
  const content = await readFile(filePath, "utf8");
  const rows = parse(content, {
    columns: true,
    skip_empty_lines: true,
    bom: true,
    trim: true,
  }) as Array<Record<string, string>>;

  if (rows.length === 0) {
    throw new Error("QA report file has no rows.");
  }

  const runId = rows[0]?.run_id;
  if (!runId) {
    throw new Error("QA report is missing run_id.");
  }

  let reviewedRows = 0;
  let passRows = 0;
  let failRows = 0;

  for (const row of rows) {
    const status = row.review_status?.trim().toLowerCase();
    if (!status) {
      continue;
    }

    reviewedRows += 1;
    if (status === "pass") {
      passRows += 1;
    } else if (status === "fail") {
      failRows += 1;
    }
  }

  const denominator = reviewedRows || 1;
  const passRate = passRows / denominator;

  return {
    runId,
    totalRows: rows.length,
    reviewedRows,
    passRows,
    failRows,
    passRate,
  };
}

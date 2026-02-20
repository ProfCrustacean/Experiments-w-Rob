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
  predicted_category_confidence: string;
  predicted_category_margin: string;
  auto_decision: "auto" | "review";
  top_confidence_reasons: string;
  needs_review: boolean;
  key_attributes: string;
  corrected_category: string;
  corrected_attributes_json: string;
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
    "predicted_category_confidence",
    "predicted_category_margin",
    "auto_decision",
    "top_confidence_reasons",
    "needs_review",
    "key_attributes",
    "corrected_category",
    "corrected_attributes_json",
    "review_status",
    "review_notes",
  ];

  const body = rows.map((row) =>
    [
      row.run_id,
      row.source_sku,
      row.title,
      row.predicted_category,
      row.predicted_category_confidence,
      row.predicted_category_margin,
      row.auto_decision,
      row.top_confidence_reasons,
      String(row.needs_review),
      row.key_attributes,
      row.corrected_category,
      row.corrected_attributes_json,
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
    predictedCategoryConfidence: number;
    predictedCategoryMargin: number;
    autoDecision: "auto" | "review";
    topConfidenceReasons: string[];
    needsReview: boolean;
    attributeValues: Record<string, unknown>;
  }>;
  sampleSize: number;
}): Promise<{
  filePath: string;
  fileName: string;
  sampledRows: number;
  totalRows: number;
  csvContent: string;
}> {
  await mkdir(input.outputDir, { recursive: true });

  const sampled = sampleWithoutReplacement(input.rows, input.sampleSize);
  const reportRows: QAReportRow[] = sampled.map((row) => ({
    run_id: input.runId,
    source_sku: row.sourceSku,
    title: row.title,
    predicted_category: row.predictedCategory,
    predicted_category_confidence: Number(row.predictedCategoryConfidence).toFixed(4),
    predicted_category_margin: Number(row.predictedCategoryMargin).toFixed(4),
    auto_decision: row.autoDecision,
    top_confidence_reasons: row.topConfidenceReasons.join(" | "),
    needs_review: row.needsReview,
    key_attributes: safeJsonString(row.attributeValues),
    corrected_category: "",
    corrected_attributes_json: "",
    review_status: "",
    review_notes: "",
  }));

  const fileName = `qa_report_${input.runId}.csv`;
  const filePath = path.join(input.outputDir, fileName);
  const csvContent = stringifyCsv(reportRows);
  await writeFile(filePath, csvContent, "utf8");

  return {
    filePath,
    fileName,
    sampledRows: reportRows.length,
    totalRows: input.rows.length,
    csvContent,
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

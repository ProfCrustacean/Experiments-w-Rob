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
  variant_signature: string;
  legacy_split_hint: string;
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
    "variant_signature",
    "legacy_split_hint",
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
      row.variant_signature,
      row.legacy_split_hint,
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

function stratifiedSampleRows<T extends { predictedCategory: string }>(
  rows: T[],
  sampleSize: number,
): T[] {
  if (sampleSize >= rows.length) {
    return [...rows];
  }

  const groups = new Map<string, T[]>();
  for (const row of rows) {
    const key = row.predictedCategory || "sem_categoria";
    const existing = groups.get(key) ?? [];
    existing.push(row);
    groups.set(key, existing);
  }

  const shuffledGroups = [...groups.entries()].map(([key, groupRows]) => ({
    key,
    rows: sampleWithoutReplacement(groupRows, groupRows.length),
  }));

  if (sampleSize <= shuffledGroups.length) {
    return sampleWithoutReplacement(shuffledGroups, sampleSize)
      .map((group) => group.rows[0])
      .filter(Boolean);
  }

  const sampled: T[] = [];
  for (const group of shuffledGroups) {
    const row = group.rows.shift();
    if (row) {
      sampled.push(row);
    }
  }

  while (sampled.length < sampleSize) {
    let added = false;
    for (const group of shuffledGroups) {
      if (sampled.length >= sampleSize) {
        break;
      }
      const row = group.rows.shift();
      if (!row) {
        continue;
      }
      sampled.push(row);
      added = true;
    }
    if (!added) {
      break;
    }
  }

  return sampled.slice(0, sampleSize);
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
    variantSignature: string;
    legacySplitHint: string;
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

  const sampled = stratifiedSampleRows(input.rows, input.sampleSize);
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
    variant_signature: row.variantSignature,
    legacy_split_hint: row.legacySplitHint,
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

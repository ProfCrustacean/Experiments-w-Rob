import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { RunLogger } from "../logging/run-logger.js";
import type { NormalizedCatalogProduct, ProductEnrichment } from "../types.js";
import { buildConfusionHotlist, type ConfusionHotlistResult } from "./confusion-hotlist.js";
import type { CategoryAssignment } from "./category-assignment.js";
import { writeQAReport } from "./qa-report.js";
import { buildVariantSignature, deriveLegacySplitHint } from "./variant-signature.js";

export interface PipelineQaRow {
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
  attributeValues: Record<string, string | number | boolean | null>;
}

export interface ReportingStageResult {
  qaRows: PipelineQaRow[];
  qaResult: {
    filePath: string;
    fileName: string;
    sampledRows: number;
    totalRows: number;
    csvContent: string;
  };
  confusionHotlist: ConfusionHotlistResult;
  confusionHotlistFileName: string;
  confusionHotlistPath: string;
}

export async function runReportingStage(input: {
  runId: string;
  outputDir: string;
  qaSampleSize: number;
  products: NormalizedCatalogProduct[];
  enrichments: Map<string, ProductEnrichment>;
  assignmentsBySku: Map<string, CategoryAssignment>;
  logger: RunLogger;
  stageTimingsMs: Record<string, number>;
}): Promise<ReportingStageResult> {
  input.logger.info("pipeline", "stage.started", "Starting QA report stage.", {
    stage_name: "qa_report",
  });

  const qaStart = Date.now();
  const qaRows: PipelineQaRow[] = input.products.map((product) => {
    const enrichment = input.enrichments.get(product.sourceSku);
    const categorySlug = enrichment?.categorySlug ?? "sem_categoria";
    const attributeValues = enrichment?.attributeValues ?? {};

    return {
      sourceSku: product.sourceSku,
      title: product.title,
      predictedCategory: categorySlug,
      predictedCategoryConfidence: enrichment?.categoryConfidence ?? 0,
      predictedCategoryMargin: enrichment?.categoryMargin ?? 0,
      autoDecision: enrichment?.autoDecision ?? "review",
      topConfidenceReasons: enrichment?.confidenceReasons ?? [],
      needsReview: enrichment?.needsReview ?? true,
      variantSignature: buildVariantSignature(attributeValues),
      legacySplitHint: deriveLegacySplitHint(categorySlug, attributeValues),
      attributeValues,
    };
  });

  const qaResult = await writeQAReport({
    outputDir: input.outputDir,
    runId: input.runId,
    rows: qaRows,
    sampleSize: input.qaSampleSize,
  });
  input.stageTimingsMs.qa_report_ms = Date.now() - qaStart;
  input.logger.info("pipeline", "stage.completed", "QA report stage completed.", {
    stage_name: "qa_report",
    elapsed_ms: input.stageTimingsMs.qa_report_ms,
    qa_sampled_rows: qaResult.sampledRows,
    qa_total_rows: qaResult.totalRows,
  });

  input.logger.info("pipeline", "stage.started", "Starting confusion hotlist stage.", {
    stage_name: "confusion_hotlist",
  });
  const confusionHotlistStart = Date.now();
  const confusionHotlist = buildConfusionHotlist({
    products: input.products,
    assignmentsBySku: input.assignmentsBySku,
    maxRows: 20,
  });
  await mkdir(input.outputDir, { recursive: true });
  const confusionHotlistFileName = `confusion_hotlist_${input.runId}.csv`;
  const confusionHotlistPath = path.join(input.outputDir, confusionHotlistFileName);
  await writeFile(confusionHotlistPath, confusionHotlist.csvContent, "utf8");
  input.stageTimingsMs.confusion_hotlist_ms = Date.now() - confusionHotlistStart;
  input.logger.info("pipeline", "stage.completed", "Confusion hotlist stage completed.", {
    stage_name: "confusion_hotlist",
    elapsed_ms: input.stageTimingsMs.confusion_hotlist_ms,
    pair_count: confusionHotlist.rows.length,
    file_name: confusionHotlistFileName,
  });

  return {
    qaRows,
    qaResult,
    confusionHotlist,
    confusionHotlistFileName,
    confusionHotlistPath,
  };
}

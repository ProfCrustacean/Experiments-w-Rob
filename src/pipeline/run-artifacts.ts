import * as XLSX from "xlsx";
import type {
  NormalizedCatalogProduct,
  PersistedCategory,
  ProductEnrichment,
  RunArtifactFormat,
  RunArtifactSummary,
} from "../types.js";
import { buildVariantSignature, deriveLegacySplitHint } from "./variant-signature.js";
import { safeJsonString } from "../utils/text.js";

export type RunArtifactKey =
  | "full_report_xlsx"
  | "full_report_csv"
  | "qa_report_csv"
  | "confusion_hotlist_csv";

export const FORMAT_TO_ARTIFACT_KEY: Record<RunArtifactFormat, RunArtifactKey> = {
  xlsx: "full_report_xlsx",
  csv: "full_report_csv",
  "qa-csv": "qa_report_csv",
  "confusion-csv": "confusion_hotlist_csv",
};

const ARTIFACT_MIME_TYPE: Record<RunArtifactKey, string> = {
  full_report_xlsx:
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  full_report_csv: "text/csv; charset=utf-8",
  qa_report_csv: "text/csv; charset=utf-8",
  confusion_hotlist_csv: "text/csv; charset=utf-8",
};

export interface RunArtifactPayload {
  key: RunArtifactKey;
  fileName: string;
  format: RunArtifactFormat;
  mimeType: string;
  content: Buffer;
  sizeBytes: number;
}

interface FullExportCsvRow {
  source_sku: string;
  title: string;
  brand: string;
  price: string;
  availability: string;
  predicted_category: string;
  category_confidence: string;
  category_margin: string;
  auto_decision: string;
  confidence_reasons: string;
  needs_review: string;
  review_reasons: string;
  variant_signature: string;
  legacy_split_hint: string;
  key_attributes: string;
  attributes_json: string;
  attribute_confidence_json: string;
  url: string;
  image_url: string;
}

interface FullExportHumanRow {
  SKU: string;
  Produto: string;
  Marca: string;
  Preco: string;
  Disponibilidade: string;
  "Categoria sugerida": string;
  "Confianca categoria": string;
  "Margem categoria": string;
  "Decisao automatica": string;
  "Razoes confianca": string;
  "Precisa revisao": string;
  "Motivos revisao": string;
  "Assinatura variante": string;
  "Hint legado": string;
  "Atributos chave": string;
  "Atributos (JSON)": string;
  "Confianca atributos (JSON)": string;
  URL: string;
  Imagem: string;
}

export function artifactKeyToFormat(key: string): RunArtifactFormat | null {
  if (key === "full_report_xlsx") {
    return "xlsx";
  }
  if (key === "full_report_csv") {
    return "csv";
  }
  if (key === "qa_report_csv") {
    return "qa-csv";
  }
  if (key === "confusion_hotlist_csv") {
    return "confusion-csv";
  }
  return null;
}

function escapeCsv(value: string): string {
  if (/[",\n]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function stringifyCsvRows(rows: FullExportCsvRow[]): string {
  const headers: Array<keyof FullExportCsvRow> = [
    "source_sku",
    "title",
    "brand",
    "price",
    "availability",
    "predicted_category",
    "category_confidence",
    "category_margin",
    "auto_decision",
    "confidence_reasons",
    "needs_review",
    "review_reasons",
    "variant_signature",
    "legacy_split_hint",
    "key_attributes",
    "attributes_json",
    "attribute_confidence_json",
    "url",
    "image_url",
  ];

  const lines = rows.map((row) => headers.map((header) => escapeCsv(row[header] ?? "")).join(","));
  return [headers.join(","), ...lines].join("\n");
}

function getKeyAttributesText(values: Record<string, unknown>): string {
  const entries = Object.entries(values)
    .filter(([, value]) => value !== null && value !== "")
    .slice(0, 6)
    .map(([key, value]) => `${key}=${String(value)}`);

  return entries.join(" | ");
}

function toFixedConfidence(value: number): string {
  return Number.isFinite(value) ? value.toFixed(4) : "0.0000";
}

function coerceNumberValue(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatDateTime(value: Date): string {
  return value.toISOString();
}

function buildProductRows(input: {
  products: NormalizedCatalogProduct[];
  enrichments: Map<string, ProductEnrichment>;
  categoriesBySlug: Map<string, PersistedCategory>;
}): { csvRows: FullExportCsvRow[]; humanRows: FullExportHumanRow[] } {
  const csvRows: FullExportCsvRow[] = [];
  const humanRows: FullExportHumanRow[] = [];

  for (const product of input.products) {
    const enrichment = input.enrichments.get(product.sourceSku);
    const categoryName = enrichment
      ? input.categoriesBySlug.get(enrichment.categorySlug)?.name_pt ?? "sem_categoria"
      : "sem_categoria";

    const attributeValues = enrichment?.attributeValues ?? {};
    const attributeConfidence = enrichment?.attributeConfidence ?? {};
    const categorySlug = enrichment?.categorySlug ?? "sem_categoria";
    const variantSignature = buildVariantSignature(attributeValues);
    const legacySplitHint = enrichment
      ? deriveLegacySplitHint(categorySlug, attributeValues)
      : "";
    const keyAttributes = getKeyAttributesText(attributeValues);
    const reasons = enrichment?.uncertaintyReasons?.join(" | ") ?? "";
    const confidenceReasons = enrichment?.confidenceReasons?.join(" | ") ?? "";
    const confidence = enrichment?.categoryConfidence ?? 0;
    const margin = enrichment?.categoryMargin ?? 0;

    const csvRow: FullExportCsvRow = {
      source_sku: product.sourceSku,
      title: product.title,
      brand: product.brand ?? "",
      price: product.price === undefined ? "" : String(product.price),
      availability: product.availability ?? "",
      predicted_category: categorySlug,
      category_confidence: toFixedConfidence(confidence),
      category_margin: toFixedConfidence(margin),
      auto_decision: enrichment?.autoDecision ?? "review",
      confidence_reasons: confidenceReasons,
      needs_review: String(enrichment?.needsReview ?? true),
      review_reasons: reasons,
      variant_signature: variantSignature,
      legacy_split_hint: legacySplitHint,
      key_attributes: keyAttributes,
      attributes_json: safeJsonString(attributeValues),
      attribute_confidence_json: safeJsonString(attributeConfidence),
      url: product.url ?? "",
      image_url: product.imageUrl ?? "",
    };

    csvRows.push(csvRow);

    humanRows.push({
      SKU: csvRow.source_sku,
      Produto: csvRow.title,
      Marca: csvRow.brand,
      Preco: csvRow.price,
      Disponibilidade: csvRow.availability,
      "Categoria sugerida": `${categoryName} (${categorySlug})`,
      "Confianca categoria": csvRow.category_confidence,
      "Margem categoria": csvRow.category_margin,
      "Decisao automatica": csvRow.auto_decision,
      "Razoes confianca": csvRow.confidence_reasons,
      "Precisa revisao": csvRow.needs_review,
      "Motivos revisao": csvRow.review_reasons,
      "Assinatura variante": csvRow.variant_signature,
      "Hint legado": csvRow.legacy_split_hint,
      "Atributos chave": csvRow.key_attributes,
      "Atributos (JSON)": csvRow.attributes_json,
      "Confianca atributos (JSON)": csvRow.attribute_confidence_json,
      URL: csvRow.url,
      Imagem: csvRow.image_url,
    });
  }

  return { csvRows, humanRows };
}

function buildSummaryRows(input: {
  runId: string;
  storeId: string;
  inputFileName: string;
  processedCount: number;
  categoryCount: number;
  needsReviewCount: number;
  autoAcceptedCount: number;
  autoAcceptedRate: number;
  fallbackCategoryCount: number;
  fallbackCategoryRate: number;
  categoryContradictionCount: number;
  attributeValidationFailCount: number;
  categoryConfidenceHistogram: Record<string, number>;
  topConfusionAlerts: Array<{
    category_slug: string;
    affected_count: number;
    low_margin_count: number;
    contradiction_count: number;
    fallback_count: number;
  }>;
  familyDistribution: Record<string, number>;
  familyReviewRate: Record<string, number>;
  variantFillRate: number;
  variantFillRateByFamily: Record<string, number>;
  taxonomyVersion: string;
  stageTimingsMs: Record<string, number>;
  startedAt: Date;
  finishedAt: Date;
  openAIEnabled: boolean;
  openAIRequestStats: unknown;
  attributeBatchFailureCount: number;
  attributeBatchFallbackProducts: number;
}): Array<{ campo: string; valor: string }> {
  const reviewRate =
    input.processedCount === 0 ? 0 : input.needsReviewCount / input.processedCount;

  const stats =
    typeof input.openAIRequestStats === "object" && input.openAIRequestStats !== null
      ? (input.openAIRequestStats as Record<string, unknown>)
      : {};

  const rows: Array<{ campo: string; valor: string }> = [
    { campo: "run_id", valor: input.runId },
    { campo: "store_id", valor: input.storeId },
    { campo: "input_file_name", valor: input.inputFileName },
    { campo: "started_at", valor: formatDateTime(input.startedAt) },
    { campo: "finished_at", valor: formatDateTime(input.finishedAt) },
    { campo: "processed_products", valor: String(input.processedCount) },
    { campo: "category_count", valor: String(input.categoryCount) },
    { campo: "needs_review_count", valor: String(input.needsReviewCount) },
    { campo: "needs_review_rate", valor: reviewRate.toFixed(4) },
    { campo: "auto_accepted_count", valor: String(input.autoAcceptedCount) },
    { campo: "auto_accepted_rate", valor: input.autoAcceptedRate.toFixed(4) },
    { campo: "fallback_category_count", valor: String(input.fallbackCategoryCount) },
    { campo: "fallback_category_rate", valor: input.fallbackCategoryRate.toFixed(4) },
    { campo: "category_contradiction_count", valor: String(input.categoryContradictionCount) },
    {
      campo: "attribute_validation_fail_count",
      valor: String(input.attributeValidationFailCount),
    },
    {
      campo: "category_confidence_histogram",
      valor: safeJsonString(input.categoryConfidenceHistogram),
    },
    {
      campo: "top_confusion_alerts",
      valor: safeJsonString(input.topConfusionAlerts),
    },
    {
      campo: "family_distribution",
      valor: safeJsonString(input.familyDistribution),
    },
    {
      campo: "family_review_rate",
      valor: safeJsonString(input.familyReviewRate),
    },
    {
      campo: "variant_fill_rate",
      valor: input.variantFillRate.toFixed(4),
    },
    {
      campo: "variant_fill_rate_by_family",
      valor: safeJsonString(input.variantFillRateByFamily),
    },
    {
      campo: "taxonomy_version",
      valor: input.taxonomyVersion,
    },
    { campo: "openai_enabled", valor: String(input.openAIEnabled) },
    {
      campo: "openai_retry_count",
      valor: String(coerceNumberValue(stats.retry_count)),
    },
    {
      campo: "openai_request_failure_count",
      valor: String(coerceNumberValue(stats.request_failure_count)),
    },
    {
      campo: "openai_timeout_count",
      valor: String(coerceNumberValue(stats.timeout_count)),
    },
    {
      campo: "attribute_batch_failure_count",
      valor: String(input.attributeBatchFailureCount),
    },
    {
      campo: "attribute_batch_fallback_products",
      valor: String(input.attributeBatchFallbackProducts),
    },
  ];

  const stageKeys = Object.keys(input.stageTimingsMs).sort();
  for (const stageKey of stageKeys) {
    rows.push({
      campo: `timing_${stageKey}`,
      valor: String(Math.round(input.stageTimingsMs[stageKey] ?? 0)),
    });
  }

  return rows;
}

function buildXlsxBuffer(input: {
  summaryRows: Array<{ campo: string; valor: string }>;
  humanRows: FullExportHumanRow[];
}): Buffer {
  const workbook = XLSX.utils.book_new();

  const summarySheet = XLSX.utils.json_to_sheet(input.summaryRows, {
    header: ["campo", "valor"],
  });
  XLSX.utils.book_append_sheet(workbook, summarySheet, "Resumo");

  const productHeaders: Array<keyof FullExportHumanRow> = [
    "SKU",
    "Produto",
    "Marca",
    "Preco",
    "Disponibilidade",
    "Categoria sugerida",
    "Confianca categoria",
    "Margem categoria",
    "Decisao automatica",
    "Razoes confianca",
    "Precisa revisao",
    "Motivos revisao",
    "Assinatura variante",
    "Hint legado",
    "Atributos chave",
    "Atributos (JSON)",
    "Confianca atributos (JSON)",
    "URL",
    "Imagem",
  ];
  const productsSheet = XLSX.utils.json_to_sheet(input.humanRows, {
    header: productHeaders,
  });
  XLSX.utils.book_append_sheet(workbook, productsSheet, "Produtos");

  const data = XLSX.write(workbook, { type: "buffer", bookType: "xlsx" });
  return Buffer.from(data);
}

function toArtifactSummary(artifact: RunArtifactPayload, expiresAt: Date): RunArtifactSummary {
  return {
    key: artifact.key,
    fileName: artifact.fileName,
    format: artifact.format,
    sizeBytes: artifact.sizeBytes,
    expiresAt: expiresAt.toISOString(),
  };
}

export function buildRunArtifacts(input: {
  runId: string;
  storeId: string;
  inputFileName: string;
  products: NormalizedCatalogProduct[];
  enrichments: Map<string, ProductEnrichment>;
  categoriesBySlug: Map<string, PersistedCategory>;
  qaReportFileName: string;
  qaReportCsvContent: string;
  confusionHotlistFileName: string;
  confusionHotlistCsvContent: string;
  categoryCount: number;
  needsReviewCount: number;
  autoAcceptedCount: number;
  autoAcceptedRate: number;
  fallbackCategoryCount: number;
  fallbackCategoryRate: number;
  categoryContradictionCount: number;
  attributeValidationFailCount: number;
  categoryConfidenceHistogram: Record<string, number>;
  topConfusionAlerts: Array<{
    category_slug: string;
    affected_count: number;
    low_margin_count: number;
    contradiction_count: number;
    fallback_count: number;
  }>;
  familyDistribution: Record<string, number>;
  familyReviewRate: Record<string, number>;
  variantFillRate: number;
  variantFillRateByFamily: Record<string, number>;
  taxonomyVersion: string;
  stageTimingsMs: Record<string, number>;
  openAIEnabled: boolean;
  openAIRequestStats: unknown;
  attributeBatchFailureCount: number;
  attributeBatchFallbackProducts: number;
  startedAt: Date;
  finishedAt: Date;
  expiresAt: Date;
}): {
  artifacts: RunArtifactPayload[];
  artifactSummaries: RunArtifactSummary[];
} {
  const rows = buildProductRows({
    products: input.products,
    enrichments: input.enrichments,
    categoriesBySlug: input.categoriesBySlug,
  });

  const summaryRows = buildSummaryRows({
    runId: input.runId,
    storeId: input.storeId,
    inputFileName: input.inputFileName,
    processedCount: input.products.length,
    categoryCount: input.categoryCount,
    needsReviewCount: input.needsReviewCount,
    autoAcceptedCount: input.autoAcceptedCount,
    autoAcceptedRate: input.autoAcceptedRate,
    fallbackCategoryCount: input.fallbackCategoryCount,
    fallbackCategoryRate: input.fallbackCategoryRate,
    categoryContradictionCount: input.categoryContradictionCount,
    attributeValidationFailCount: input.attributeValidationFailCount,
    categoryConfidenceHistogram: input.categoryConfidenceHistogram,
    topConfusionAlerts: input.topConfusionAlerts,
    familyDistribution: input.familyDistribution,
    familyReviewRate: input.familyReviewRate,
    variantFillRate: input.variantFillRate,
    variantFillRateByFamily: input.variantFillRateByFamily,
    taxonomyVersion: input.taxonomyVersion,
    stageTimingsMs: input.stageTimingsMs,
    startedAt: input.startedAt,
    finishedAt: input.finishedAt,
    openAIEnabled: input.openAIEnabled,
    openAIRequestStats: input.openAIRequestStats,
    attributeBatchFailureCount: input.attributeBatchFailureCount,
    attributeBatchFallbackProducts: input.attributeBatchFallbackProducts,
  });

  const fullCsvContent = stringifyCsvRows(rows.csvRows);
  const fullCsvArtifact: RunArtifactPayload = {
    key: FORMAT_TO_ARTIFACT_KEY.csv,
    fileName: `pipeline_output_${input.runId}.csv`,
    format: "csv",
    mimeType: ARTIFACT_MIME_TYPE.full_report_csv,
    content: Buffer.from(fullCsvContent, "utf8"),
    sizeBytes: Buffer.byteLength(fullCsvContent, "utf8"),
  };

  const xlsxBuffer = buildXlsxBuffer({ summaryRows, humanRows: rows.humanRows });
  const fullXlsxArtifact: RunArtifactPayload = {
    key: FORMAT_TO_ARTIFACT_KEY.xlsx,
    fileName: `pipeline_output_${input.runId}.xlsx`,
    format: "xlsx",
    mimeType: ARTIFACT_MIME_TYPE.full_report_xlsx,
    content: xlsxBuffer,
    sizeBytes: xlsxBuffer.byteLength,
  };

  const qaContent = input.qaReportCsvContent;
  const qaArtifact: RunArtifactPayload = {
    key: FORMAT_TO_ARTIFACT_KEY["qa-csv"],
    fileName: input.qaReportFileName,
    format: "qa-csv",
    mimeType: ARTIFACT_MIME_TYPE.qa_report_csv,
    content: Buffer.from(qaContent, "utf8"),
    sizeBytes: Buffer.byteLength(qaContent, "utf8"),
  };

  const confusionContent = input.confusionHotlistCsvContent;
  const confusionHotlistArtifact: RunArtifactPayload = {
    key: FORMAT_TO_ARTIFACT_KEY["confusion-csv"],
    fileName: input.confusionHotlistFileName,
    format: "confusion-csv",
    mimeType: ARTIFACT_MIME_TYPE.confusion_hotlist_csv,
    content: Buffer.from(confusionContent, "utf8"),
    sizeBytes: Buffer.byteLength(confusionContent, "utf8"),
  };

  const artifacts = [fullXlsxArtifact, fullCsvArtifact, qaArtifact, confusionHotlistArtifact];
  const artifactSummaries = artifacts.map((artifact) =>
    toArtifactSummary(artifact, input.expiresAt),
  );

  return {
    artifacts,
    artifactSummaries,
  };
}

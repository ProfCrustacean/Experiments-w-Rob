import * as XLSX from "xlsx";
import { describe, expect, it } from "vitest";
import { artifactKeyToFormat, buildRunArtifacts } from "../src/pipeline/run-artifacts.js";
import type { PersistedCategory, ProductEnrichment } from "../src/types.js";

describe("run artifacts", () => {
  it("maps known artifact keys to download formats", () => {
    expect(artifactKeyToFormat("full_report_xlsx")).toBe("xlsx");
    expect(artifactKeyToFormat("full_report_csv")).toBe("csv");
    expect(artifactKeyToFormat("qa_report_csv")).toBe("qa-csv");
    expect(artifactKeyToFormat("confusion_hotlist_csv")).toBe("confusion-csv");
    expect(artifactKeyToFormat("unknown")).toBeNull();
  });

  it("builds xlsx/csv/qa-csv artifacts with expected structure", () => {
    const products = [
      {
        sourceSku: "sku-1",
        title: "Caderno A4",
        description: "Caderno pautado",
        brand: "Note!",
        price: 2.99,
        availability: "in_stock",
        url: "https://example.com/p1",
        imageUrl: "https://example.com/p1.jpg",
        normalizedTitle: "caderno a4",
        normalizedDescription: "caderno pautado",
        normalizedBrand: "note",
        normalizedText: "caderno a4 pautado",
      },
    ];

    const enrichments = new Map<string, ProductEnrichment>([
      [
        "sku-1",
        {
          sourceSku: "sku-1",
          categorySlug: "cadernos_blocos",
          categoryConfidence: 0.92,
          categoryTop2Confidence: 0.75,
          categoryMargin: 0.17,
          autoDecision: "auto",
          confidenceReasons: ["family_assignment", "strong_lexical_match"],
          isFallbackCategory: false,
          categoryContradictionCount: 0,
          attributeValidationFailCount: 0,
          attributeValues: { item_subtype: "caderno", format: "A4", ruling: "pautado" },
          attributeConfidence: { item_subtype: 0.9, format: 0.9, ruling: 0.88 },
          needsReview: false,
          uncertaintyReasons: [],
        },
      ],
    ]);

    const categories = new Map<string, PersistedCategory>([
      [
        "cadernos_blocos",
        {
          id: "cat-1",
          slug: "cadernos_blocos",
          name_pt: "Cadernos e Blocos",
          description_pt: "Categoria de cadernos e blocos.",
          attributes_jsonb: {
            schema_version: "1.0",
            category_name_pt: "cadernos e blocos",
            attributes: [],
          },
        },
      ],
    ]);

    const now = new Date("2026-02-20T20:00:00.000Z");
    const expiresAt = new Date("2026-02-21T20:00:00.000Z");

    const result = buildRunArtifacts({
      runId: "run-123",
      storeId: "continente",
      inputFileName: "output.csv",
      products,
      enrichments,
      categoriesBySlug: categories,
      qaReportFileName: "qa_report_run-123.csv",
      qaReportCsvContent:
        "run_id,source_sku,title,predicted_category,needs_review,key_attributes,review_status,review_notes\n",
      confusionHotlistFileName: "confusion_hotlist_run-123.csv",
      confusionHotlistCsvContent:
        "category_a,category_b,affected_count,low_margin_count,contradiction_count,sample_skus,sample_titles,top_tokens,suggested_include_a,suggested_exclude_a,suggested_include_b,suggested_exclude_b\n",
      categoryCount: 1,
      needsReviewCount: 0,
      autoAcceptedCount: 1,
      autoAcceptedRate: 1,
      fallbackCategoryCount: 0,
      fallbackCategoryRate: 0,
      categoryContradictionCount: 0,
      attributeValidationFailCount: 0,
      categoryConfidenceHistogram: { "0.8-1.0": 1 },
      topConfusionAlerts: [],
      familyDistribution: { cadernos_blocos: 1 },
      familyReviewRate: { cadernos_blocos: 0 },
      variantFillRate: 1,
      variantFillRateByFamily: { cadernos_blocos: 1 },
      taxonomyVersion: "2.0|2.0",
      stageTimingsMs: { enrichment_ms: 100, embedding_ms: 200 },
      openAIEnabled: true,
      openAIRequestStats: { retry_count: 2 },
      attributeBatchFailureCount: 0,
      attributeBatchFallbackProducts: 0,
      startedAt: now,
      finishedAt: now,
      expiresAt,
    });

    expect(result.artifacts).toHaveLength(4);
    for (const artifact of result.artifacts) {
      expect(artifact.sizeBytes).toBeGreaterThan(0);
    }

    const csvArtifact = result.artifacts.find((artifact) => artifact.key === "full_report_csv");
    expect(csvArtifact).toBeTruthy();
    expect(csvArtifact?.content.toString("utf8")).toContain(
      "source_sku,title,brand,price,availability,predicted_category,category_confidence,category_margin,auto_decision,confidence_reasons,needs_review,review_reasons,variant_signature,legacy_split_hint",
    );

    const xlsxArtifact = result.artifacts.find((artifact) => artifact.key === "full_report_xlsx");
    expect(xlsxArtifact).toBeTruthy();

    const workbook = XLSX.read(xlsxArtifact?.content, { type: "buffer" });
    expect(workbook.SheetNames).toContain("Resumo");
    expect(workbook.SheetNames).toContain("Produtos");

    const qaArtifact = result.artifacts.find((artifact) => artifact.key === "qa_report_csv");
    expect(qaArtifact?.fileName).toBe("qa_report_run-123.csv");

    const confusionArtifact = result.artifacts.find(
      (artifact) => artifact.key === "confusion_hotlist_csv",
    );
    expect(confusionArtifact?.fileName).toBe("confusion_hotlist_run-123.csv");

    expect(result.artifactSummaries).toHaveLength(4);
    expect(result.artifactSummaries[0]?.expiresAt).toBe(expiresAt.toISOString());
  });
});

import { describe, expect, it } from "vitest";
import { buildConfusionHotlist } from "../src/pipeline/confusion-hotlist.js";

describe("confusion hotlist", () => {
  it("builds ranked confusion rows for low-margin family pairs", () => {
    const products = [
      {
        sourceSku: "sku-1",
        title: "Pasta arquivo A4 com etiquetas",
        description: "Classificador escolar",
        brand: "Marca",
        normalizedTitle: "pasta arquivo a4 com etiquetas",
        normalizedDescription: "classificador escolar",
        normalizedBrand: "marca",
        normalizedText: "pasta arquivo a4 com etiquetas classificador escolar marca",
      },
      {
        sourceSku: "sku-2",
        title: "Etiquetas autocolantes para caderno",
        description: "Bloco aderente escolar",
        brand: "Marca",
        normalizedTitle: "etiquetas autocolantes para caderno",
        normalizedDescription: "bloco aderente escolar",
        normalizedBrand: "marca",
        normalizedText: "etiquetas autocolantes para caderno bloco aderente escolar marca",
      },
    ];

    const assignmentsBySku = new Map([
      [
        "sku-1",
        {
          sourceSku: "sku-1",
          categorySlug: "organizacao_arquivo",
          categoryTop2Slug: "cadernos_blocos",
          categoryConfidence: 0.62,
          categoryTop2Confidence: 0.58,
          categoryMargin: 0.04,
          autoDecision: "review" as const,
          confidenceReasons: ["family_assignment", "low_margin"],
          isFallbackCategory: false,
          categoryContradictionCount: 0,
          lexicalScore: 0.6,
          semanticScore: 0.55,
          attributeCompatibilityScore: 0.5,
        },
      ],
      [
        "sku-2",
        {
          sourceSku: "sku-2",
          categorySlug: "organizacao_arquivo",
          categoryTop2Slug: "cadernos_blocos",
          categoryConfidence: 0.63,
          categoryTop2Confidence: 0.57,
          categoryMargin: 0.06,
          autoDecision: "review" as const,
          confidenceReasons: ["family_assignment", "low_margin"],
          isFallbackCategory: false,
          categoryContradictionCount: 1,
          lexicalScore: 0.62,
          semanticScore: 0.56,
          attributeCompatibilityScore: 0.52,
        },
      ],
    ]);

    const result = buildConfusionHotlist({
      products,
      assignmentsBySku,
      maxRows: 20,
    });

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]?.category_a).toBe("cadernos_blocos");
    expect(result.rows[0]?.category_b).toBe("organizacao_arquivo");
    expect(result.rows[0]?.affected_count).toBe(2);
    expect(result.csvContent).toContain("suggested_include_a");
  });
});

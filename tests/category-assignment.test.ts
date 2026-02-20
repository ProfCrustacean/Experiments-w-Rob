import { describe, expect, it } from "vitest";
import { normalizeRows } from "../src/pipeline/ingest.js";
import { assignCategoriesForProducts } from "../src/pipeline/category-assignment.js";
import { FallbackProvider } from "../src/services/fallback.js";

describe("category assignment", () => {
  it("avoids assigning sharpeners to lapis-cor", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-afia",
        title: "Afia com Depósito Maped",
        description: "Afia escolar com depósito duplo",
        brand: "Maped",
      },
      {
        sourceSku: "sku-lapis",
        title: "Lápis de Cor 24 Unidades",
        description: "Caixa com 24 cores",
        brand: "Faber-Castell",
      },
    ]);

    const provider = new FallbackProvider(256);
    const output = await assignCategoriesForProducts({
      products,
      embeddingProvider: provider,
      llmProvider: provider,
      autoMinConfidence: 0.76,
      autoMinMargin: 0.1,
      highRiskExtraConfidence: 0.08,
      llmConcurrency: 2,
      embeddingBatchSize: 8,
      embeddingConcurrency: 2,
    });

    expect(output.assignmentsBySku.get("sku-afia")?.categorySlug).toBe("afia");
    expect(output.assignmentsBySku.get("sku-lapis")?.categorySlug).toBe("lapis-cor");
  });

  it("forces fallback categories to review", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-unknown",
        title: "Acessório Escolar Decorativo",
        description: "Item sortido sem subcategoria clara",
        brand: "Note!",
      },
    ]);

    const provider = new FallbackProvider(256);
    const output = await assignCategoriesForProducts({
      products,
      embeddingProvider: provider,
      llmProvider: provider,
      autoMinConfidence: 0.76,
      autoMinMargin: 0.1,
      highRiskExtraConfidence: 0.08,
      llmConcurrency: 1,
      embeddingBatchSize: 8,
      embeddingConcurrency: 2,
    });

    const assignment = output.assignmentsBySku.get("sku-unknown");
    expect(assignment?.isFallbackCategory).toBe(true);
    expect(assignment?.autoDecision).toBe("review");
  });

  it("routes low-margin ambiguous cases to review", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-amb",
        title: "Caneta Gel Esferográfica 1.0mm",
        description: "Caneta para escrita escolar",
        brand: "Generic",
      },
    ]);

    const provider = new FallbackProvider(256);
    const output = await assignCategoriesForProducts({
      products,
      embeddingProvider: provider,
      llmProvider: provider,
      autoMinConfidence: 0.76,
      autoMinMargin: 0.1,
      highRiskExtraConfidence: 0.08,
      llmConcurrency: 1,
      embeddingBatchSize: 8,
      embeddingConcurrency: 2,
    });

    const assignment = output.assignmentsBySku.get("sku-amb");
    expect(assignment).toBeTruthy();
    expect(assignment?.autoDecision).toBe("review");
  });
});

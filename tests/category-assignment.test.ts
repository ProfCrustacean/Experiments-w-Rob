import { describe, expect, it } from "vitest";
import { normalizeRows } from "../src/pipeline/ingest.js";
import { assignCategoriesForProducts } from "../src/pipeline/category-assignment.js";
import { FallbackProvider } from "../src/services/fallback.js";

class FallbackRescueEmbeddingProvider {
  public readonly dimensions = 2;

  async embedMany(texts: string[]): Promise<number[][]> {
    return texts.map((text) => {
      const normalized = text.toLowerCase();
      if (normalized.includes("outros escolares")) {
        return [1, 0];
      }
      if (normalized.includes("escrita")) {
        return [0.6, 0.8];
      }
      if (normalized.includes("sem-subcategoria")) {
        return [1, 0];
      }
      return [0, 1];
    });
  }
}

describe("category assignment", () => {
  it("assigns school notebook products to cadernos_blocos", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-caderno",
        title: "Caderno A5 Pautado 96 folhas",
        description: "Caderno brochura para escola",
        brand: "Oxford",
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

    const assignment = output.assignmentsBySku.get("sku-caderno");
    expect(assignment?.categorySlug).toBe("cadernos_blocos");
    expect(assignment?.confidenceReasons).toContain("family_assignment");
  });

  it("classifies filing products as organizacao_arquivo", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-classificador",
        title: "Classificador com Mola Clip A4",
        description: "Pasta para arquivo escolar",
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

    expect(output.assignmentsBySku.get("sku-classificador")?.categorySlug).toBe("organizacao_arquivo");
  });

  it("routes tape products to cola_adesivos", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-fita",
        title: "Fita Adesiva Transparente 2 Unidades",
        description: "Fita para trabalhos escolares",
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

    expect(output.assignmentsBySku.get("sku-fita")?.categorySlug).toBe("cola_adesivos");
  });

  it("routes non-school products to fora_escopo_escolar", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-oos",
        title: "Auriculares Wireless Bluetooth",
        description: "Acessorio para telemovel",
        brand: "Marshmallow",
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

    const assignment = output.assignmentsBySku.get("sku-oos");
    expect(assignment?.categorySlug).toBe("fora_escopo_escolar");
    expect(assignment?.autoDecision).toBe("review");
    expect(assignment?.confidenceReasons).toContain("out_of_scope_category");
  });

  it("keeps fallback categories in review", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-unknown",
        title: "Acessorio Escolar Decorativo",
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
    expect(assignment?.categorySlug).toBe("outros_escolares");
    expect(assignment?.isFallbackCategory).toBe(true);
    expect(assignment?.autoDecision).toBe("review");
  });

  it("rescues fallback predictions to a specific family when evidence is strong", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-fallback-rescue",
        title: "Item sem-subcategoria caneta azul 1.0mm",
        description: "Produto de escrita",
        brand: "Marca Gen",
      },
    ]);

    const embeddingProvider = new FallbackRescueEmbeddingProvider();
    const output = await assignCategoriesForProducts({
      products,
      embeddingProvider,
      llmProvider: null,
      autoMinConfidence: 0.76,
      autoMinMargin: 0.1,
      highRiskExtraConfidence: 0.08,
      llmConcurrency: 1,
      embeddingBatchSize: 8,
      embeddingConcurrency: 2,
    });

    const assignment = output.assignmentsBySku.get("sku-fallback-rescue");
    expect(assignment?.categorySlug).toBe("escrita");
    expect(assignment?.isFallbackCategory).toBe(false);
    expect(assignment?.autoDecision).toBe("review");
    expect(assignment?.confidenceReasons).toContain("family_assignment");
  });
});

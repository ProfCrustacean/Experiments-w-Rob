import { describe, expect, it } from "vitest";
import { normalizeRows } from "../src/pipeline/ingest.js";
import { assignCategoriesForProducts } from "../src/pipeline/category-assignment.js";
import { FallbackProvider } from "../src/services/fallback.js";

class FallbackRescueEmbeddingProvider {
  public readonly dimensions = 2;

  async embedMany(texts: string[]): Promise<number[][]> {
    return texts.map((text) => {
      const normalized = text.toLowerCase();
      if (normalized.includes("material escolar diverso")) {
        return [1, 0];
      }
      if (normalized.includes("caneta esferografica")) {
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

  it("keeps file organizer products away from afia and classifies as pasta-arquivo", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-classificador",
        title: "Classificador com Mola Clip A4",
        description: "Pasta e classificador para arquivo escolar",
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

    const assignment = output.assignmentsBySku.get("sku-classificador");
    expect(assignment?.categorySlug).toBe("pasta-arquivo");
    expect(assignment?.categorySlug).not.toBe("afia");
  });

  it("locks caderno subtype when explicit format/ruling evidence exists", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-caderno-locked",
        title: "Caderno A5 Pautado 96 folhas",
        description: "Caderno brochura pautado para escola",
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

    const assignment = output.assignmentsBySku.get("sku-caderno-locked");
    expect(assignment?.categorySlug).toBe("caderno-a5-pautado");
    expect(assignment?.confidenceReasons).toContain("caderno_subtype_lock");
    expect(assignment?.autoDecision).toBe("auto");
  });

  it("classifies standalone esferografica titles as caneta-esferografica", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-esfero",
        title: "Esferográfica Cristal 1mm Azul",
        description: "Tinta azul para escrita diaria",
        brand: "BIC",
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

    const assignment = output.assignmentsBySku.get("sku-esfero");
    expect(assignment?.categorySlug).toBe("caneta-esferografica");
    expect(assignment?.categorySlug).not.toBe("material-escolar-diverso");
  });

  it("routes tape products to fita-adesiva instead of cola families", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-fita-adesiva",
        title: "Fita Adesiva Transparente 2 Unidades",
        description: "Fita cola para trabalhos escolares",
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

    const assignment = output.assignmentsBySku.get("sku-fita-adesiva");
    expect(assignment?.categorySlug).toBe("fita-adesiva");
    expect(assignment?.categorySlug).not.toBe("cola-liquida");
    expect(assignment?.categorySlug).not.toBe("cola-bastao");
  });

  it("routes non-core products to out-of-scope review category", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-oos",
        title: "Auriculares Wireless Marshmallow",
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
    expect(assignment?.categorySlug).toBe("fora-escopo-escolar");
    expect(assignment?.autoDecision).toBe("review");
    expect(assignment?.confidenceReasons).toContain("out_of_scope_category");
  });

  it("rescues fallback predictions to a specific review category when secondary evidence is strong", async () => {
    const products = normalizeRows([
      {
        sourceSku: "sku-fallback-rescue",
        title: "Item sem-subcategoria 1.0mm preta",
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
    expect(assignment?.categorySlug).toBe("caneta-esferografica");
    expect(assignment?.isFallbackCategory).toBe(false);
    expect(assignment?.autoDecision).toBe("review");
    expect(assignment?.confidenceReasons).toContain("fallback_rescue_applied");
  });
});

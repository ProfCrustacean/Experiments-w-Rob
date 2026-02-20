import { describe, expect, it } from "vitest";
import { enrichProduct } from "../src/pipeline/enrichment.js";

describe("enrichment", () => {
  it("extracts only schema-defined attributes", async () => {
    const product = {
      sourceSku: "sku-1",
      title: "Caderno A4 pautado",
      description: "96 folhas",
      brand: "Marca X",
      normalizedTitle: "caderno a4 pautado",
      normalizedDescription: "96 folhas",
      normalizedBrand: "marca x",
      normalizedText: "caderno a4 pautado 96 folhas marca x",
    };

    const category = {
      slug: "caderno-a4-pautado",
      description: "Categoria de cadernos escolares",
      confidenceScore: 0.92,
      attributes: {
        schema_version: "1.0" as const,
        category_name_pt: "caderno a4 pautado",
        attributes: [
          {
            key: "format",
            label_pt: "formato",
            type: "enum" as const,
            allowed_values: ["A4", "A5"],
            required: false,
          },
          {
            key: "ruling",
            label_pt: "tipo_de_folha",
            type: "enum" as const,
            allowed_values: ["pautado", "quadriculado", "liso"],
            required: false,
          },
        ],
      },
    };

    const result = await enrichProduct(product, category, null, 0.7);

    expect(Object.keys(result.attributeValues).sort()).toEqual(["format", "ruling"]);
    expect(result.attributeValues.format).toBe("A4");
    expect(result.attributeValues.ruling).toBe("pautado");
    expect(result.needsReview).toBe(false);
  });

  it("flags uncertain products for review", async () => {
    const product = {
      sourceSku: "sku-2",
      title: "Material escolar diverso",
      description: "",
      brand: "",
      normalizedTitle: "material escolar diverso",
      normalizedDescription: "",
      normalizedBrand: "",
      normalizedText: "material escolar diverso",
    };

    const category = {
      slug: "material-diverso",
      description: "Categoria genérica",
      confidenceScore: 0.41,
      attributes: {
        schema_version: "1.0" as const,
        category_name_pt: "material escolar diverso",
        attributes: [
          {
            key: "pack_count",
            label_pt: "quantidade_no_pack",
            type: "number" as const,
            required: true,
          },
        ],
      },
    };

    const result = await enrichProduct(product, category, null, 0.7);

    expect(result.needsReview).toBe(true);
    expect(result.uncertaintyReasons).toContain("low_category_confidence");
  });
});

import { describe, expect, it } from "vitest";
import { FallbackProvider } from "../src/services/fallback.js";

describe("LLM batch extraction contract", () => {
  it("returns one result per requested source_sku", async () => {
    const provider = new FallbackProvider();
    const output = await provider.extractProductAttributesBatch({
      categoryName: "caderno a4 pautado",
      categoryDescription: "cadernos escolares pautados",
      attributeSchema: {
        schema_version: "1.0",
        category_name_pt: "caderno a4 pautado",
        attributes: [
          {
            key: "format",
            label_pt: "formato",
            type: "enum",
            allowed_values: ["A4", "A5"],
            required: false,
          },
          {
            key: "ruling",
            label_pt: "tipo_de_folha",
            type: "enum",
            allowed_values: ["pautado", "quadriculado", "liso"],
            required: false,
          },
        ],
      },
      products: [
        {
          sourceSku: "sku-1",
          product: {
            title: "Caderno A4 pautado",
            description: "96 folhas",
            brand: "Marca A",
          },
        },
        {
          sourceSku: "sku-2",
          product: {
            title: "Caderno A5 quadriculado",
            description: "80 folhas",
            brand: "Marca B",
          },
        },
      ],
    });

    expect(Object.keys(output).sort()).toEqual(["sku-1", "sku-2"]);
    expect(Object.keys(output["sku-1"].values).sort()).toEqual(["format", "ruling"]);
    expect(output["sku-1"].values.format).toBeNull();
    expect(output["sku-1"].confidence.format).toBe(0);
  });
});

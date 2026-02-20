import { describe, expect, it } from "vitest";
import { buildExactSchemaShape, generateCategoryDrafts } from "../src/pipeline/category-generation.js";

describe("category generation", () => {
  it("produces exact category schema structure", () => {
    const schema = buildExactSchemaShape("Caderno A4 Pautado", [
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
    ]);

    expect(schema.schema_version).toBe("1.0");
    expect(schema.category_name_pt).toBe("caderno a4 pautado");
    expect(schema.attributes[0].key).toBe("format");
  });

  it("creates fallback drafts when no LLM provider is present", async () => {
    const { drafts } = await generateCategoryDrafts(
      [
        {
          key: "caderno-a4-pautado",
          candidateName: "Caderno A4 pautado",
          products: [
            {
              sourceSku: "sku-1",
              title: "Caderno A4 pautado",
              normalizedTitle: "caderno a4 pautado",
              normalizedDescription: "96 folhas",
              normalizedBrand: "",
              normalizedText: "caderno a4 pautado 96 folhas",
            },
          ],
          scoresBySku: { "sku-1": 0.9 },
        },
      ],
      null,
    );

    expect(drafts).toHaveLength(1);
    expect(drafts[0].name_pt.length).toBeLessThanOrEqual(42);
    expect(drafts[0].description_pt.length).toBeGreaterThan(10);
    expect(drafts[0].attributes_jsonb.attributes.length).toBeGreaterThan(0);
  });
});

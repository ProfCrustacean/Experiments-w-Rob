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

  it("does not force review when auto decision only has positive confidence signals", async () => {
    const product = {
      sourceSku: "sku-3",
      title: "Agrafador Mini Escolar",
      description: "Agrafador pequeno para secretária",
      brand: "Note!",
      normalizedTitle: "agrafador mini escolar",
      normalizedDescription: "agrafador pequeno para secretaria",
      normalizedBrand: "note",
      normalizedText: "agrafador mini escolar agrafador pequeno para secretaria note",
    };

    const category = {
      slug: "agrafador",
      description: "Categoria de agrafadores",
      confidenceScore: 0.9,
      autoDecision: "auto" as const,
      confidenceReasons: ["strong_lexical_match", "strong_semantic_match"],
      attributes: {
        schema_version: "1.0" as const,
        category_name_pt: "agrafador",
        attributes: [],
      },
    };

    const result = await enrichProduct(product, category, null, 0.7);

    expect(result.autoDecision).toBe("auto");
    expect(result.needsReview).toBe(false);
    expect(result.uncertaintyReasons).toEqual([]);
  });

  it("applies deterministic parsing for mochila, estojo, marcador and cola liquida", async () => {
    const baseCategory = {
      description: "Categoria",
      confidenceScore: 0.9,
      autoDecision: "auto" as const,
      confidenceReasons: ["strong_lexical_match"],
    };

    const mochilaResult = await enrichProduct(
      {
        sourceSku: "sku-mochila",
        title: "Mochila Trolley 25L Infantil",
        description: "",
        brand: "Marca",
        normalizedTitle: "mochila trolley 25l infantil",
        normalizedDescription: "",
        normalizedBrand: "marca",
        normalizedText: "mochila trolley 25l infantil",
      },
      {
        ...baseCategory,
        slug: "mochila",
        attributes: {
          schema_version: "1.0" as const,
          category_name_pt: "mochila",
          attributes: [
            { key: "has_wheels", label_pt: "tem_rodas", type: "boolean" as const, required: false },
            { key: "capacity_l", label_pt: "capacidade_litros", type: "number" as const, required: false },
            { key: "target_age", label_pt: "faixa_etaria", type: "text" as const, required: false },
          ],
        },
      },
      null,
      0.7,
    );

    expect(mochilaResult.attributeValues.has_wheels).toBe(true);
    expect(mochilaResult.attributeValues.capacity_l).toBe(25);
    expect(mochilaResult.attributeValues.target_age).toBe("infantil");

    const estojoResult = await enrichProduct(
      {
        sourceSku: "sku-estojo",
        title: "Estojo Triplo Silicone Azul",
        description: "",
        brand: "Marca",
        normalizedTitle: "estojo triplo silicone azul",
        normalizedDescription: "",
        normalizedBrand: "marca",
        normalizedText: "estojo triplo silicone azul",
      },
      {
        ...baseCategory,
        slug: "estojo",
        attributes: {
          schema_version: "1.0" as const,
          category_name_pt: "estojo",
          attributes: [
            { key: "compartment_count", label_pt: "numero_de_compartimentos", type: "number" as const, required: false },
            { key: "material", label_pt: "material", type: "text" as const, required: false },
          ],
        },
      },
      null,
      0.7,
    );

    expect(estojoResult.attributeValues.compartment_count).toBe(3);
    expect(estojoResult.attributeValues.material).toBe("silicone");

    const marcadorResult = await enrichProduct(
      {
        sourceSku: "sku-marcador",
        title: "Marcador Fine 0.4 5 unidades",
        description: "",
        brand: "Marca",
        normalizedTitle: "marcador fine 0.4 5 unidades",
        normalizedDescription: "",
        normalizedBrand: "marca",
        normalizedText: "marcador fine 0.4 5 unidades",
      },
      {
        ...baseCategory,
        slug: "marcador-texto",
        attributes: {
          schema_version: "1.0" as const,
          category_name_pt: "marcador",
          attributes: [
            {
              key: "tip_type",
              label_pt: "tipo_de_ponta",
              type: "enum" as const,
              allowed_values: ["chanfrada", "fina", "media"],
              required: false,
            },
            { key: "pack_count", label_pt: "quantidade_no_pack", type: "number" as const, required: false },
          ],
        },
      },
      null,
      0.7,
    );

    expect(marcadorResult.attributeValues.tip_type).toBe("fina");
    expect(marcadorResult.attributeValues.pack_count).toBe(5);

    const colaResult = await enrichProduct(
      {
        sourceSku: "sku-cola",
        title: "Cola Liquida Branca 110g",
        description: "",
        brand: "Marca",
        normalizedTitle: "cola liquida branca 110g",
        normalizedDescription: "",
        normalizedBrand: "marca",
        normalizedText: "cola liquida branca 110g",
      },
      {
        ...baseCategory,
        slug: "cola-liquida",
        attributes: {
          schema_version: "1.0" as const,
          category_name_pt: "cola liquida",
          attributes: [
            {
              key: "glue_type",
              label_pt: "tipo_de_cola",
              type: "enum" as const,
              allowed_values: ["bastao", "liquida"],
              required: false,
            },
            { key: "volume_ml", label_pt: "volume_ml", type: "number" as const, required: false },
          ],
        },
      },
      null,
      0.7,
    );

    expect(colaResult.attributeValues.glue_type).toBe("liquida");
    expect(colaResult.attributeValues.volume_ml).toBe(110);
  });

  it("does not force review when optional attributes are empty", async () => {
    const result = await enrichProduct(
      {
        sourceSku: "sku-opt",
        title: "Estojo Escolar Simples",
        description: "",
        brand: "Marca",
        normalizedTitle: "estojo escolar simples",
        normalizedDescription: "",
        normalizedBrand: "marca",
        normalizedText: "estojo escolar simples marca",
      },
      {
        slug: "estojo",
        description: "Categoria de estojos",
        confidenceScore: 0.88,
        autoDecision: "auto" as const,
        confidenceReasons: ["strong_lexical_match"],
        attributes: {
          schema_version: "1.0" as const,
          category_name_pt: "estojo",
          attributes: [
            { key: "compartment_count", label_pt: "numero_de_compartimentos", type: "number" as const, required: false },
            { key: "material", label_pt: "material", type: "text" as const, required: false },
          ],
        },
      },
      null,
      0.7,
    );

    expect(result.attributeValues.compartment_count).toBe(1);
    expect(result.needsReview).toBe(false);
    expect(result.uncertaintyReasons).not.toContain("empty_attribute_output");
  });

  it("keeps mochila without wheel signal as auto when other gating is safe", async () => {
    const result = await enrichProduct(
      {
        sourceSku: "sku-mochila-sem-rodas",
        title: "Mochila Escolar Azul 22L",
        description: "Modelo infantil com alcas reforcadas",
        brand: "Marca",
        normalizedTitle: "mochila escolar azul 22l",
        normalizedDescription: "modelo infantil com alcas reforcadas",
        normalizedBrand: "marca",
        normalizedText: "mochila escolar azul 22l modelo infantil com alcas reforcadas marca",
      },
      {
        slug: "mochila",
        description: "Categoria de mochilas",
        confidenceScore: 0.9,
        autoDecision: "auto" as const,
        confidenceReasons: ["strong_lexical_match", "strong_semantic_match"],
        attributes: {
          schema_version: "1.0" as const,
          category_name_pt: "mochila",
          attributes: [
            { key: "has_wheels", label_pt: "tem_rodas", type: "boolean" as const, required: false },
            { key: "capacity_l", label_pt: "capacidade_litros", type: "number" as const, required: false },
          ],
        },
      },
      null,
      0.7,
    );

    expect(result.attributeValues.has_wheels).toBeNull();
    expect(result.needsReview).toBe(false);
    expect(result.uncertaintyReasons).not.toContain("low_attribute_confidence_has_wheels");
  });

  it("does not infer glue_type from tape products", async () => {
    const result = await enrichProduct(
      {
        sourceSku: "sku-fita",
        title: "Fita Adesiva Transparente 2 unidades",
        description: "",
        brand: "Marca",
        normalizedTitle: "fita adesiva transparente 2 unidades",
        normalizedDescription: "",
        normalizedBrand: "marca",
        normalizedText: "fita adesiva transparente 2 unidades marca",
      },
      {
        slug: "cola-liquida",
        description: "Categoria de cola liquida",
        confidenceScore: 0.82,
        autoDecision: "review" as const,
        confidenceReasons: ["category_review_gate"],
        attributes: {
          schema_version: "1.0" as const,
          category_name_pt: "cola liquida",
          attributes: [
            {
              key: "glue_type",
              label_pt: "tipo_de_cola",
              type: "enum" as const,
              allowed_values: ["bastao", "liquida"],
              required: false,
            },
          ],
        },
      },
      null,
      0.7,
    );

    expect(result.attributeValues.glue_type).toBeNull();
    expect(result.uncertaintyReasons).not.toContain("contradiction_glue_type_liquida");
  });
});

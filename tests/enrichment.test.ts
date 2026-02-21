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
      slug: "cadernos_blocos",
      description: "Categoria de cadernos e blocos",
      confidenceScore: 0.92,
      autoDecision: "auto" as const,
      confidenceReasons: ["family_assignment"],
      attributes: {
        schema_version: "1.0" as const,
        category_name_pt: "cadernos e blocos",
        attributes: [
          {
            key: "item_subtype",
            label_pt: "subtipo",
            type: "enum" as const,
            allowed_values: ["caderno", "bloco_apontamentos"],
            required: false,
          },
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

    expect(Object.keys(result.attributeValues).sort()).toEqual(["format", "item_subtype", "ruling"]);
    expect(result.attributeValues.item_subtype).toBe("caderno");
    expect(result.attributeValues.format).toBe("A4");
    expect(result.attributeValues.ruling).toBe("pautado");
    expect(result.needsReview).toBe(false);
  });

  it("flags uncertain products for review", async () => {
    const product = {
      sourceSku: "sku-2",
      title: "Item escolar diverso",
      description: "",
      brand: "",
      normalizedTitle: "item escolar diverso",
      normalizedDescription: "",
      normalizedBrand: "",
      normalizedText: "item escolar diverso",
    };

    const category = {
      slug: "outros_escolares",
      description: "Categoria genérica",
      confidenceScore: 0.41,
      autoDecision: "review" as const,
      confidenceReasons: ["below_auto_confidence", "generic_or_fallback_category"],
      attributes: {
        schema_version: "1.0" as const,
        category_name_pt: "outros escolares",
        attributes: [
          {
            key: "item_subtype",
            label_pt: "subtipo",
            type: "enum" as const,
            allowed_values: ["calculadora", "acessorio_escolar", "kit_escolar"],
            required: false,
          },
        ],
      },
    };

    const result = await enrichProduct(product, category, null, 0.7);

    expect(result.needsReview).toBe(true);
    expect(result.uncertaintyReasons).toContain("low_category_confidence");
  });

  it("does not force review when auto decision has safe signals", async () => {
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
      slug: "organizacao_arquivo",
      description: "Categoria de organização",
      confidenceScore: 0.9,
      autoDecision: "auto" as const,
      confidenceReasons: ["family_assignment", "strong_lexical_match"],
      attributes: {
        schema_version: "1.0" as const,
        category_name_pt: "organizacao e arquivo",
        attributes: [
          {
            key: "item_subtype",
            label_pt: "subtipo",
            type: "enum" as const,
            allowed_values: ["agrafador", "agrafos"],
            required: false,
          },
        ],
      },
    };

    const result = await enrichProduct(product, category, null, 0.7);

    expect(result.autoDecision).toBe("auto");
    expect(result.needsReview).toBe(false);
    expect(result.uncertaintyReasons).toEqual([]);
    expect(result.attributeValues.item_subtype).toBe("agrafador");
  });

  it("applies deterministic parsing for transporte_escolar, escrita and cola_adesivos", async () => {
    const baseCategory = {
      description: "Categoria",
      confidenceScore: 0.9,
      autoDecision: "auto" as const,
      confidenceReasons: ["family_assignment"],
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
        slug: "transporte_escolar",
        attributes: {
          schema_version: "1.0" as const,
          category_name_pt: "transporte escolar",
          attributes: [
            { key: "item_subtype", label_pt: "subtipo", type: "enum" as const, allowed_values: ["mochila", "estojo"], required: false },
            { key: "has_wheels", label_pt: "tem_rodas", type: "boolean" as const, required: false },
            { key: "capacity_l", label_pt: "capacidade_litros", type: "number" as const, required: false },
            { key: "target_age", label_pt: "faixa_etaria", type: "text" as const, required: false },
          ],
        },
      },
      null,
      0.7,
    );

    expect(mochilaResult.attributeValues.item_subtype).toBe("mochila");
    expect(mochilaResult.attributeValues.has_wheels).toBe(true);
    expect(mochilaResult.attributeValues.capacity_l).toBe(25);
    expect(mochilaResult.attributeValues.target_age).toBe("infantil");

    const escritaResult = await enrichProduct(
      {
        sourceSku: "sku-escrita",
        title: "Caneta Gel 0.7mm Pack 12",
        description: "",
        brand: "Marca",
        normalizedTitle: "caneta gel 0.7mm pack 12",
        normalizedDescription: "",
        normalizedBrand: "marca",
        normalizedText: "caneta gel 0.7mm pack 12",
      },
      {
        ...baseCategory,
        slug: "escrita",
        attributes: {
          schema_version: "1.0" as const,
          category_name_pt: "escrita",
          attributes: [
            { key: "item_subtype", label_pt: "subtipo", type: "enum" as const, allowed_values: ["caneta", "lapis_grafite"], required: false },
            { key: "ink_type", label_pt: "tipo_de_tinta", type: "enum" as const, allowed_values: ["gel", "esferografica"], required: false },
            { key: "point_size_mm", label_pt: "ponta", type: "number" as const, required: false },
            { key: "pack_count", label_pt: "pack", type: "number" as const, required: false },
          ],
        },
      },
      null,
      0.7,
    );

    expect(escritaResult.attributeValues.item_subtype).toBe("caneta");
    expect(escritaResult.attributeValues.ink_type).toBe("gel");
    expect(escritaResult.attributeValues.point_size_mm).toBe(0.7);
    expect(escritaResult.attributeValues.pack_count).toBe(12);

    const colaResult = await enrichProduct(
      {
        sourceSku: "sku-cola",
        title: "Cola Liquida Branca 110ml",
        description: "",
        brand: "Marca",
        normalizedTitle: "cola liquida branca 110ml",
        normalizedDescription: "",
        normalizedBrand: "marca",
        normalizedText: "cola liquida branca 110ml",
      },
      {
        ...baseCategory,
        slug: "cola_adesivos",
        attributes: {
          schema_version: "1.0" as const,
          category_name_pt: "cola e adesivos",
          attributes: [
            { key: "item_subtype", label_pt: "subtipo", type: "enum" as const, allowed_values: ["cola", "fita"], required: false },
            { key: "glue_type", label_pt: "tipo_de_cola", type: "enum" as const, allowed_values: ["bastao", "liquida"], required: false },
            { key: "volume_ml", label_pt: "volume_ml", type: "number" as const, required: false },
          ],
        },
      },
      null,
      0.7,
    );

    expect(colaResult.attributeValues.item_subtype).toBe("cola");
    expect(colaResult.attributeValues.glue_type).toBe("liquida");
    expect(colaResult.attributeValues.volume_ml).toBe(110);
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
        slug: "cola_adesivos",
        description: "Categoria de cola e adesivos",
        confidenceScore: 0.86,
        autoDecision: "review" as const,
        confidenceReasons: ["category_review_gate"],
        attributes: {
          schema_version: "1.0" as const,
          category_name_pt: "cola e adesivos",
          attributes: [
            { key: "item_subtype", label_pt: "subtipo", type: "enum" as const, allowed_values: ["cola", "fita"], required: false },
            { key: "glue_type", label_pt: "tipo_de_cola", type: "enum" as const, allowed_values: ["bastao", "liquida"], required: false },
            { key: "tape_type", label_pt: "tipo_de_fita", type: "enum" as const, allowed_values: ["transparente", "dupla_face", "papel"], required: false },
          ],
        },
      },
      null,
      0.7,
    );

    expect(result.attributeValues.item_subtype).toBe("fita");
    expect(result.attributeValues.glue_type).toBeNull();
    expect(result.attributeValues.tape_type).toBe("transparente");
  });
});

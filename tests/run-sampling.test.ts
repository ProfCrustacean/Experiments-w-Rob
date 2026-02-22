import { describe, expect, it } from "vitest";
import {
  __test_only_buildEscalationAttributeKeys,
  __test_only_isEnrichmentImproved,
  __test_only_normalizeRowsForPipeline,
  __test_only_partitionProductsBySample,
} from "../src/pipeline/run-test-support.js";
import type { ProductEnrichment } from "../src/types.js";

describe("run sampling", () => {
  it("selects a deterministic partition for the same store and settings", () => {
    const rows = __test_only_normalizeRowsForPipeline([
      { sourceSku: "sku-1", title: "Produto 1" },
      { sourceSku: "sku-2", title: "Produto 2" },
      { sourceSku: "sku-3", title: "Produto 3" },
      { sourceSku: "sku-4", title: "Produto 4" },
      { sourceSku: "sku-5", title: "Produto 5" },
      { sourceSku: "sku-6", title: "Produto 6" },
    ]);

    const first = __test_only_partitionProductsBySample(rows, "continente", 2, 0);
    const second = __test_only_partitionProductsBySample(rows, "continente", 2, 0);

    expect(first.sampled.map((row) => row.sourceSku)).toEqual(
      second.sampled.map((row) => row.sourceSku),
    );
    expect(first.skipped).toBe(second.skipped);
  });

  it("creates complementary halves for part 0 and part 1", () => {
    const rows = __test_only_normalizeRowsForPipeline([
      { sourceSku: "sku-1", title: "Produto 1" },
      { sourceSku: "sku-2", title: "Produto 2" },
      { sourceSku: "sku-3", title: "Produto 3" },
      { sourceSku: "sku-4", title: "Produto 4" },
      { sourceSku: "sku-5", title: "Produto 5" },
      { sourceSku: "sku-6", title: "Produto 6" },
      { sourceSku: "sku-7", title: "Produto 7" },
      { sourceSku: "sku-8", title: "Produto 8" },
    ]);

    const firstHalf = __test_only_partitionProductsBySample(rows, "continente", 2, 0);
    const secondHalf = __test_only_partitionProductsBySample(rows, "continente", 2, 1);

    const firstSkus = new Set(firstHalf.sampled.map((row) => row.sourceSku));
    const secondSkus = new Set(secondHalf.sampled.map((row) => row.sourceSku));

    for (const sku of firstSkus) {
      expect(secondSkus.has(sku)).toBe(false);
    }

    expect(firstSkus.size + secondSkus.size).toBe(rows.length);
  });

  it("builds escalation keys only for uncertain attribute signals", () => {
    const keys = __test_only_buildEscalationAttributeKeys({
      contextAttributes: {
        schema_version: "1.0",
        category_name_pt: "escrita",
        attributes: [
          {
            key: "item_subtype",
            label_pt: "Subtipo",
            type: "enum",
            allowed_values: ["caneta", "lapis_grafite"],
            required: true,
          },
          {
            key: "point_size_mm",
            label_pt: "Ponta",
            type: "number",
            required: false,
          },
        ],
      },
      enrichment: {
        sourceSku: "sku-1",
        categorySlug: "escrita",
        categoryConfidence: 0.8,
        categoryTop2Confidence: 0.6,
        categoryMargin: 0.2,
        autoDecision: "review",
        confidenceReasons: [],
        isFallbackCategory: false,
        categoryContradictionCount: 0,
        attributeValidationFailCount: 0,
        attributeValues: {
          item_subtype: null,
          point_size_mm: 0.7,
        },
        attributeConfidence: {
          item_subtype: 0.2,
          point_size_mm: 0.4,
        },
        needsReview: true,
        uncertaintyReasons: ["missing_required_item_subtype", "low_optional_attribute_confidence_point_size_mm"],
      },
      requiredMinConfidence: 0.8,
      optionalMinConfidence: 0.65,
    });

    expect(keys).toEqual(["item_subtype", "point_size_mm"]);
  });

  it("does not escalate when uncertainty is unrelated to attribute extraction", () => {
    const keys = __test_only_buildEscalationAttributeKeys({
      contextAttributes: {
        schema_version: "1.0",
        category_name_pt: "escrita",
        attributes: [
          {
            key: "item_subtype",
            label_pt: "Subtipo",
            type: "enum",
            allowed_values: ["caneta", "lapis_grafite"],
            required: true,
          },
        ],
      },
      enrichment: {
        sourceSku: "sku-1",
        categorySlug: "escrita",
        categoryConfidence: 0.45,
        categoryTop2Confidence: 0.4,
        categoryMargin: 0.05,
        autoDecision: "review",
        confidenceReasons: ["low_margin"],
        isFallbackCategory: false,
        categoryContradictionCount: 0,
        attributeValidationFailCount: 0,
        attributeValues: {
          item_subtype: "caneta",
        },
        attributeConfidence: {
          item_subtype: 0.91,
        },
        needsReview: true,
        uncertaintyReasons: ["low_category_confidence"],
      },
      requiredMinConfidence: 0.8,
      optionalMinConfidence: 0.65,
    });

    expect(keys).toEqual([]);
  });

  it("maps policy and contradiction reasons to escalation keys", () => {
    const keys = __test_only_buildEscalationAttributeKeys({
      contextAttributes: {
        schema_version: "1.0",
        category_name_pt: "escrita",
        attributes: [
          {
            key: "item_subtype",
            label_pt: "Subtipo",
            type: "enum",
            allowed_values: ["caneta", "lapis_grafite"],
            required: true,
          },
          {
            key: "ink_type",
            label_pt: "Tipo de Tinta",
            type: "enum",
            allowed_values: ["esferografica", "gel"],
            required: false,
          },
          {
            key: "pack_count",
            label_pt: "Quantidade no Pack",
            type: "number",
            required: false,
          },
          {
            key: "sheet_count",
            label_pt: "Numero de Folhas",
            type: "number",
            required: false,
          },
          {
            key: "length_cm",
            label_pt: "Comprimento (cm)",
            type: "number",
            required: false,
          },
        ],
      },
      enrichment: {
        sourceSku: "sku-2",
        categorySlug: "escrita",
        categoryConfidence: 0.82,
        categoryTop2Confidence: 0.72,
        categoryMargin: 0.1,
        autoDecision: "review",
        confidenceReasons: [],
        isFallbackCategory: false,
        categoryContradictionCount: 0,
        attributeValidationFailCount: 3,
        attributeValues: {
          item_subtype: null,
          ink_type: "esferografica",
          pack_count: 999,
          sheet_count: 10,
          length_cm: 0.2,
        },
        attributeConfidence: {
          item_subtype: 0.2,
          ink_type: 0.9,
          pack_count: 0.88,
          sheet_count: 0.88,
          length_cm: 0.88,
        },
        needsReview: true,
        uncertaintyReasons: [
          "policy_min_length_cm",
          "policy_pack_context_missing",
          "contradiction_ink_type_for_lapis",
          "pack_count_remapped_to_sheet_count",
          "missing_variant_for_auto",
        ],
      },
      requiredMinConfidence: 0.85,
      optionalMinConfidence: 0.7,
    });

    expect(keys).toEqual(["ink_type", "item_subtype", "length_cm", "pack_count", "sheet_count"]);
  });

  it("prefers second-pass output only when enrichment quality improves", () => {
    const current: ProductEnrichment = {
      sourceSku: "sku-1",
      categorySlug: "escrita",
      categoryConfidence: 0.8,
      categoryTop2Confidence: 0.6,
      categoryMargin: 0.2,
      autoDecision: "review",
      confidenceReasons: [],
      isFallbackCategory: false,
      categoryContradictionCount: 0,
      attributeValidationFailCount: 0,
      attributeValues: { item_subtype: null, point_size_mm: null },
      attributeConfidence: { item_subtype: 0.2, point_size_mm: 0.1 },
      needsReview: true,
      uncertaintyReasons: ["missing_required_item_subtype", "empty_attribute_output"],
    };

    const improved: ProductEnrichment = {
      ...current,
      attributeValues: { item_subtype: "caneta", point_size_mm: 0.7 },
      attributeConfidence: { item_subtype: 0.92, point_size_mm: 0.82 },
      needsReview: false,
      uncertaintyReasons: [],
    };

    const worse: ProductEnrichment = {
      ...current,
      attributeValues: { item_subtype: null, point_size_mm: 0.7 },
      attributeConfidence: { item_subtype: 0.2, point_size_mm: 0.35 },
      needsReview: true,
      uncertaintyReasons: ["missing_required_item_subtype", "low_optional_attribute_confidence_point_size_mm", "empty_attribute_output"],
    };

    expect(__test_only_isEnrichmentImproved(current, improved)).toBe(true);
    expect(__test_only_isEnrichmentImproved(current, worse)).toBe(false);
  });
});

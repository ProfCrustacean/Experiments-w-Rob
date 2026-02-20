import type {
  CategoryAttribute,
  CategoryAttributeSchema,
  LLMProvider,
  NormalizedCatalogProduct,
  ProductEnrichment,
} from "../types.js";
import {
  detectFormat,
  detectPackCount,
  detectRuling,
  detectNumericQuantity,
  normalizeText,
} from "../utils/text.js";

function parseNumber(text: string, regex: RegExp): number | null {
  const match = normalizeText(text).match(regex);
  if (!match) {
    return null;
  }

  const value = Number(match[1].replace(",", "."));
  return Number.isFinite(value) ? value : null;
}

function detectBoolean(text: string, positive: RegExp, negative: RegExp): boolean | null {
  const normalized = normalizeText(text);
  if (negative.test(normalized)) {
    return false;
  }
  if (positive.test(normalized)) {
    return true;
  }
  return null;
}

function inferAttributeWithRules(
  attribute: CategoryAttribute,
  product: NormalizedCatalogProduct,
): { value: string | number | boolean | null; confidence: number } {
  const text = `${product.normalizedTitle} ${product.normalizedDescription}`;

  switch (attribute.key) {
    case "format": {
      const format = detectFormat(text);
      return { value: format, confidence: format ? 0.92 : 0.1 };
    }
    case "ruling": {
      const ruling = detectRuling(text);
      return { value: ruling, confidence: ruling ? 0.9 : 0.1 };
    }
    case "pack_count": {
      const packCount = detectPackCount(text) ?? detectNumericQuantity(text);
      return { value: packCount, confidence: packCount ? 0.88 : 0.1 };
    }
    case "sheet_count": {
      const sheetCount = parseNumber(text, /(\d{2,3})\s*(?:folhas|fls)/);
      return { value: sheetCount, confidence: sheetCount ? 0.86 : 0.1 };
    }
    case "point_size_mm": {
      const pointSize = parseNumber(text, /(\d(?:[.,]\d)?)\s*mm/);
      return { value: pointSize, confidence: pointSize ? 0.86 : 0.1 };
    }
    case "hardness": {
      const hardness = text.match(/\b(HB|2B|B|H)\b/i)?.[1]?.toUpperCase() ?? null;
      return { value: hardness, confidence: hardness ? 0.84 : 0.1 };
    }
    case "glue_type": {
      if (/(bastao|stick)/.test(text)) {
        return { value: "bastao", confidence: 0.85 };
      }
      if (/(liquida|liquido|liquid)/.test(text)) {
        return { value: "liquida", confidence: 0.85 };
      }
      return { value: null, confidence: 0.1 };
    }
    case "volume_ml": {
      const volume = parseNumber(text, /(\d{1,4})\s*ml/);
      return { value: volume, confidence: volume ? 0.84 : 0.1 };
    }
    case "has_wheels": {
      const value = detectBoolean(text, /(com rodas|rodas)/, /(sem rodas)/);
      return { value, confidence: value !== null ? 0.86 : 0.1 };
    }
    case "capacity_l": {
      const capacity = parseNumber(text, /(\d{1,3})\s*(?:l|litros)/);
      return { value: capacity, confidence: capacity ? 0.84 : 0.1 };
    }
    case "tip_safety": {
      const tipSafety = detectBoolean(text, /(ponta redonda|seguranca)/, /(ponta afiada)/);
      return { value: tipSafety, confidence: tipSafety !== null ? 0.82 : 0.1 };
    }
    case "length_cm": {
      const length = parseNumber(text, /(\d{1,3})\s*cm/);
      return { value: length, confidence: length ? 0.83 : 0.1 };
    }
    default:
      return { value: null, confidence: 0.05 };
  }
}

function normalizeEnumValue(attribute: CategoryAttribute, rawValue: unknown): string | null {
  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return null;
  }

  const input = normalizeText(String(rawValue));
  if (!attribute.allowed_values || attribute.allowed_values.length === 0) {
    return input || null;
  }

  const matched = attribute.allowed_values.find((allowed) => normalizeText(allowed) === input);
  if (matched) {
    return matched;
  }

  const partial = attribute.allowed_values.find((allowed) => {
    const normalizedAllowed = normalizeText(allowed);
    return normalizedAllowed.includes(input) || input.includes(normalizedAllowed);
  });

  return partial ?? null;
}

function coerceValue(attribute: CategoryAttribute, value: unknown): string | number | boolean | null {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  if (attribute.type === "enum") {
    return normalizeEnumValue(attribute, value);
  }

  if (attribute.type === "number") {
    const parsed = Number(String(value).replace(",", "."));
    return Number.isFinite(parsed) ? parsed : null;
  }

  if (attribute.type === "boolean") {
    if (typeof value === "boolean") {
      return value;
    }
    const normalized = normalizeText(String(value));
    if (["sim", "true", "1", "yes", "com"].includes(normalized)) {
      return true;
    }
    if (["nao", "não", "false", "0", "no", "sem"].includes(normalized)) {
      return false;
    }
    return null;
  }

  return String(value).trim() || null;
}

function sanitizeConfidence(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Math.max(0, Math.min(1, parsed));
}

function hasAnyAttributeValue(values: Record<string, string | number | boolean | null>): boolean {
  return Object.values(values).some((value) => value !== null && value !== "");
}

export async function enrichProduct(
  product: NormalizedCatalogProduct,
  category: {
    slug: string;
    attributes: CategoryAttributeSchema;
    description: string;
    confidenceScore: number;
  },
  llm: LLMProvider | null,
  confidenceThreshold: number,
): Promise<ProductEnrichment> {
  const ruleValues: Record<string, string | number | boolean | null> = {};
  const ruleConfidence: Record<string, number> = {};

  for (const attribute of category.attributes.attributes) {
    const inferred = inferAttributeWithRules(attribute, product);
    ruleValues[attribute.key] = coerceValue(attribute, inferred.value);
    ruleConfidence[attribute.key] = sanitizeConfidence(inferred.confidence);
  }

  let llmValues: Record<string, string | number | boolean | null> = {};
  let llmConfidence: Record<string, number> = {};

  if (llm) {
    try {
      const response = await llm.extractProductAttributes({
        product: {
          title: product.title,
          description: product.description,
          brand: product.brand,
        },
        categoryName: category.attributes.category_name_pt,
        categoryDescription: category.description,
        attributeSchema: category.attributes,
      });

      llmValues = response.values;
      llmConfidence = response.confidence;
    } catch {
      llmValues = {};
      llmConfidence = {};
    }
  }

  const attributeValues: Record<string, string | number | boolean | null> = {};
  const attributeConfidence: Record<string, number> = {};
  const reasons: string[] = [];

  for (const attribute of category.attributes.attributes) {
    const key = attribute.key;

    const llmValue = coerceValue(attribute, llmValues[key]);
    const llmScore = sanitizeConfidence(llmConfidence[key]);

    const ruleValue = coerceValue(attribute, ruleValues[key]);
    const ruleScore = sanitizeConfidence(ruleConfidence[key]);

    if (ruleValue !== null && ruleScore >= llmScore) {
      attributeValues[key] = ruleValue;
      attributeConfidence[key] = ruleScore;
    } else {
      attributeValues[key] = llmValue;
      attributeConfidence[key] = llmScore;
    }

    if (attribute.required && (attributeValues[key] === null || attributeValues[key] === "")) {
      reasons.push(`missing_required_${key}`);
    }

    if (
      attribute.type === "enum" &&
      attributeValues[key] !== null &&
      attribute.allowed_values &&
      attribute.allowed_values.length > 0
    ) {
      const normalized = normalizeText(String(attributeValues[key]));
      const valid = attribute.allowed_values.some(
        (allowed) => normalizeText(allowed) === normalized,
      );
      if (!valid) {
        reasons.push(`invalid_enum_${key}`);
        attributeValues[key] = null;
        attributeConfidence[key] = Math.min(attributeConfidence[key], 0.2);
      }
    }
  }

  if (!hasAnyAttributeValue(attributeValues) && category.attributes.attributes.length > 0) {
    reasons.push("empty_attribute_output");
  }

  if (category.confidenceScore < confidenceThreshold) {
    reasons.push("low_category_confidence");
  }

  const needsReview = reasons.length > 0;

  return {
    sourceSku: product.sourceSku,
    categorySlug: category.slug,
    categoryConfidence: category.confidenceScore,
    attributeValues,
    attributeConfidence,
    needsReview,
    uncertaintyReasons: reasons,
  };
}

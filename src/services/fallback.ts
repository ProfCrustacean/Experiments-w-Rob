import type {
  AttributeExtractionLLMOutput,
  CategoryProfileLLMOutput,
  EmbeddingProvider,
  LLMProvider,
} from "../types.js";
import { CATEGORY_HINT_RULES, FALLBACK_CATEGORY_RULE } from "../pipeline/category-rules.js";
import { normalizeText, shortSpecificName, tokenize } from "../utils/text.js";

function hashString(value: string): number {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return Math.abs(hash >>> 0);
}

function normalizeVector(values: number[]): number[] {
  const sumSquares = values.reduce((sum, value) => sum + value * value, 0);
  if (sumSquares === 0) {
    return values;
  }
  const norm = Math.sqrt(sumSquares);
  return values.map((value) => value / norm);
}

export class FallbackProvider implements EmbeddingProvider, LLMProvider {
  public readonly dimensions: number;

  constructor(dimensions = 3072) {
    this.dimensions = dimensions;
  }

  async embedMany(texts: string[]): Promise<number[][]> {
    return texts.map((text) => {
      const vector = new Array<number>(this.dimensions).fill(0);
      for (const token of tokenize(text)) {
        const index = hashString(token) % this.dimensions;
        vector[index] += 1;
      }
      return normalizeVector(vector);
    });
  }

  async generateCategoryProfile(input: {
    candidateName: string;
    sampleProducts: Array<{ title: string; description?: string; brand?: string }>;
  }): Promise<CategoryProfileLLMOutput> {
    const mergedText = input.sampleProducts
      .map((item) => `${item.title} ${item.description ?? ""}`)
      .join(" ")
      .toLowerCase();

    const matchedRule =
      CATEGORY_HINT_RULES.find((rule) =>
        rule.keywords.some((keyword) => normalizeText(mergedText).includes(normalizeText(keyword))),
      ) ?? FALLBACK_CATEGORY_RULE;

    const name = shortSpecificName(input.candidateName || matchedRule.labelPt);

    return {
      name_pt: name,
      description_pt: `${name} para material escolar. Subcategoria criada automaticamente com base no catálogo.`,
      synonyms: [...matchedRule.synonyms, name],
      attributes: matchedRule.defaultAttributes,
    };
  }

  async extractProductAttributes(input: {
    attributeSchema: {
      schema_version: "1.0";
      category_name_pt: string;
      attributes: Array<{
        key: string;
        label_pt: string;
        type: "enum" | "number" | "boolean" | "text";
        allowed_values?: string[];
        required: boolean;
      }>;
    };
  }): Promise<AttributeExtractionLLMOutput> {
    const values: Record<string, string | number | boolean | null> = {};
    const confidence: Record<string, number> = {};

    for (const attribute of input.attributeSchema.attributes) {
      values[attribute.key] = null;
      confidence[attribute.key] = 0;
    }

    return { values, confidence };
  }
}

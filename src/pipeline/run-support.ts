import { getConfig } from "../config.js";
import { FallbackProvider } from "../services/fallback.js";
import { OpenAIProvider, type OpenAITelemetryCallback } from "../services/openai.js";
import type {
  CategoryAttributeSchema,
  EmbeddingProvider,
  LLMProvider,
  NormalizedCatalogProduct,
  ProductEnrichment,
} from "../types.js";

export const EMBEDDING_DIMENSIONS = 1536;

export interface RunPipelineInput {
  inputPath: string;
  storeId: string;
  runLabel?: string;
  sampleParts?: number;
  samplePartIndex?: number;
}

export interface CategoryContext {
  slug: string;
  attributes: CategoryAttributeSchema;
  description: string;
  confidenceScore: number;
  top2Confidence: number;
  margin: number;
  autoDecision: "auto" | "review";
  confidenceReasons: string[];
  isFallbackCategory: boolean;
  contradictionCount: number;
}

export interface AttributeBatchItem {
  product: NormalizedCatalogProduct;
  context: CategoryContext;
  llmAttributeSchema: CategoryContext["attributes"];
}

export interface AttributeBatchTask {
  categorySlug: string;
  categoryContext: Pick<CategoryContext, "slug" | "description">;
  llmAttributeSchema: CategoryContext["attributes"];
  items: AttributeBatchItem[];
}

export function ensureVectorLength(vector: number[], target: number): number[] {
  if (vector.length === target) {
    return vector;
  }

  if (vector.length > target) {
    return vector.slice(0, target);
  }

  return [...vector, ...new Array(target - vector.length).fill(0)];
}

function hashString(value: string): number {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return Math.abs(hash >>> 0);
}

export function shouldLogProgress(done: number, total: number): boolean {
  if (total <= 0) {
    return false;
  }
  if (done === total) {
    return true;
  }
  return done === 1 || done % 10 === 0;
}

export function hasVariantValues(values: Record<string, string | number | boolean | null>): boolean {
  return Object.entries(values).some(([key, value]) => {
    if (key === "item_subtype") {
      return false;
    }
    return value !== null && value !== "";
  });
}

function inferEscalationKeysFromReason(reason: string, availableKeys: Set<string>): Set<string> {
  const keys = new Set<string>();

  const directSuffixPrefixes = [
    "missing_required_",
    "low_attribute_confidence_",
    "low_optional_attribute_confidence_",
    "invalid_enum_",
    "invalid_boolean_",
    "invalid_number_",
    "invalid_text_",
  ] as const;

  for (const prefix of directSuffixPrefixes) {
    if (!reason.startsWith(prefix)) {
      continue;
    }
    const key = reason.slice(prefix.length);
    if (availableKeys.has(key)) {
      keys.add(key);
    }
  }

  const contradictionMatch = reason.match(/^contradiction_([a-z0-9_]+?)(?:_for_|$)/);
  if (contradictionMatch) {
    const candidateKey = contradictionMatch[1];
    if (availableKeys.has(candidateKey)) {
      keys.add(candidateKey);
    }
  }

  const policyBoundMatch = reason.match(/^policy_(?:min|max)_([a-z0-9_]+)$/);
  if (policyBoundMatch) {
    const candidateKey = policyBoundMatch[1];
    if (availableKeys.has(candidateKey)) {
      keys.add(candidateKey);
    }
  }

  if (reason === "policy_pack_context_missing" && availableKeys.has("pack_count")) {
    keys.add("pack_count");
  }

  if (reason === "pack_count_remapped_to_sheet_count") {
    if (availableKeys.has("pack_count")) {
      keys.add("pack_count");
    }
    if (availableKeys.has("sheet_count")) {
      keys.add("sheet_count");
    }
  }

  if (reason === "missing_variant_for_auto" && availableKeys.has("item_subtype")) {
    keys.add("item_subtype");
  }

  for (const key of availableKeys) {
    if (reason.includes(key)) {
      keys.add(key);
    }
  }

  return keys;
}

function extractEscalationKeysFromReasons(input: {
  uncertaintyReasons: string[];
  availableKeys: Set<string>;
  requiredKeys: Set<string>;
}): Set<string> {
  const keys = new Set<string>();

  for (const reason of input.uncertaintyReasons) {
    if (reason === "empty_attribute_output") {
      for (const requiredKey of input.requiredKeys) {
        keys.add(requiredKey);
      }
      continue;
    }

    const inferred = inferEscalationKeysFromReason(reason, input.availableKeys);
    for (const key of inferred) {
      keys.add(key);
    }
  }

  return keys;
}

export function buildEscalationAttributeSchema(input: {
  context: CategoryContext;
  enrichment: ProductEnrichment;
  requiredMinConfidence: number;
  optionalMinConfidence: number;
}): CategoryContext["attributes"] | null {
  if (!input.enrichment.needsReview) {
    return null;
  }

  const availableKeys = new Set(input.context.attributes.attributes.map((attribute) => attribute.key));
  const requiredKeys = new Set(
    input.context.attributes.attributes
      .filter((attribute) => attribute.required)
      .map((attribute) => attribute.key),
  );
  const keys = extractEscalationKeysFromReasons({
    uncertaintyReasons: input.enrichment.uncertaintyReasons,
    availableKeys,
    requiredKeys,
  });

  for (const attribute of input.context.attributes.attributes) {
    const value = input.enrichment.attributeValues[attribute.key];
    const confidence = input.enrichment.attributeConfidence[attribute.key] ?? 0;
    if ((value === null || value === "") && attribute.required) {
      keys.add(attribute.key);
      continue;
    }
    if (
      attribute.required &&
      value !== null &&
      value !== "" &&
      confidence < input.requiredMinConfidence
    ) {
      keys.add(attribute.key);
      continue;
    }
    if (
      !attribute.required &&
      value !== null &&
      value !== "" &&
      confidence < input.optionalMinConfidence
    ) {
      keys.add(attribute.key);
    }
  }

  if (keys.size === 0) {
    return null;
  }

  return {
    ...input.context.attributes,
    attributes: input.context.attributes.attributes.filter((attribute) => keys.has(attribute.key)),
  };
}

function countFilledAttributes(values: Record<string, string | number | boolean | null>): number {
  return Object.values(values).filter((value) => value !== null && value !== "").length;
}

function averageAttributeConfidence(confidence: Record<string, number>): number {
  const values = Object.values(confidence).filter((value) => Number.isFinite(value));
  if (values.length === 0) {
    return 0;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

export function isEnrichmentImproved(current: ProductEnrichment, candidate: ProductEnrichment): boolean {
  if (current.needsReview && !candidate.needsReview) {
    return true;
  }

  const currentReasonCount = current.uncertaintyReasons.length;
  const candidateReasonCount = candidate.uncertaintyReasons.length;
  if (candidateReasonCount < currentReasonCount) {
    return true;
  }
  if (candidateReasonCount > currentReasonCount) {
    return false;
  }

  const currentFilled = countFilledAttributes(current.attributeValues);
  const candidateFilled = countFilledAttributes(candidate.attributeValues);
  if (candidateFilled > currentFilled) {
    return true;
  }
  if (candidateFilled < currentFilled) {
    return false;
  }

  return averageAttributeConfidence(candidate.attributeConfidence) >
    averageAttributeConfidence(current.attributeConfidence) + 0.03;
}

export function partitionProductsBySample(
  products: NormalizedCatalogProduct[],
  storeId: string,
  sampleParts: number,
  samplePartIndex: number,
): {
  sampled: NormalizedCatalogProduct[];
  skipped: number;
} {
  if (sampleParts <= 1) {
    return {
      sampled: products,
      skipped: 0,
    };
  }

  const sampled: NormalizedCatalogProduct[] = [];
  let skipped = 0;

  for (const product of products) {
    const partition = hashString(`${storeId}::${product.sourceSku}`) % sampleParts;
    if (partition === samplePartIndex) {
      sampled.push(product);
    } else {
      skipped += 1;
    }
  }

  return {
    sampled,
    skipped,
  };
}

export function validateSamplingInput(sampleParts: number, samplePartIndex: number): void {
  if (!Number.isInteger(sampleParts) || sampleParts <= 0) {
    throw new Error(`Invalid sample-parts value: ${sampleParts}. Expected a positive integer.`);
  }
  if (!Number.isInteger(samplePartIndex) || samplePartIndex < 0) {
    throw new Error(
      `Invalid sample-part-index value: ${samplePartIndex}. Expected a non-negative integer.`,
    );
  }
  if (samplePartIndex >= sampleParts) {
    throw new Error(
      `Invalid sampling configuration: sample-part-index (${samplePartIndex}) must be smaller than sample-parts (${sampleParts}).`,
    );
  }
}

export function createProviders(openAITelemetry?: OpenAITelemetryCallback): {
  embeddingProvider: EmbeddingProvider;
  llmProvider: LLMProvider;
  usingOpenAI: boolean;
  openAIProvider: OpenAIProvider | null;
} {
  const config = getConfig();

  if (config.OPENAI_API_KEY) {
    const openAIProvider = new OpenAIProvider({
      apiKey: config.OPENAI_API_KEY,
      llmModel: config.LLM_MODEL,
      embeddingModel: config.EMBEDDING_MODEL,
      dimensions: EMBEDDING_DIMENSIONS,
      timeoutMs: config.OPENAI_TIMEOUT_MS,
      maxRetries: config.OPENAI_MAX_RETRIES,
      retryBaseMs: config.OPENAI_RETRY_BASE_MS,
      retryMaxMs: config.OPENAI_RETRY_MAX_MS,
      telemetry: openAITelemetry,
    });

    return {
      embeddingProvider: openAIProvider,
      llmProvider: openAIProvider,
      usingOpenAI: true,
      openAIProvider,
    };
  }

  const fallback = new FallbackProvider(EMBEDDING_DIMENSIONS);
  return {
    embeddingProvider: fallback,
    llmProvider: fallback,
    usingOpenAI: false,
    openAIProvider: null,
  };
}

export function buildMissingCategoryEnrichment(sourceSku: string): ProductEnrichment {
  return {
    sourceSku,
    categorySlug: "",
    categoryConfidence: 0,
    categoryTop2Confidence: 0,
    categoryMargin: 0,
    autoDecision: "review",
    confidenceReasons: ["missing_category"],
    isFallbackCategory: true,
    categoryContradictionCount: 0,
    attributeValidationFailCount: 0,
    attributeValues: {},
    attributeConfidence: {},
    needsReview: true,
    uncertaintyReasons: ["missing_category"],
  };
}

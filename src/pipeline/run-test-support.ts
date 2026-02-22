import { normalizeRows } from "./ingest.js";
import type {
  EmbeddingProvider,
  LLMProvider,
  NormalizedCatalogProduct,
  ProductEnrichment,
} from "../types.js";
import {
  type CategoryContext,
  buildEscalationAttributeSchema,
  createProviders,
  ensureVectorLength,
  isEnrichmentImproved,
  partitionProductsBySample,
} from "./run-support.js";

export function __test_only_createProviders(): {
  embeddingProvider: EmbeddingProvider;
  llmProvider: LLMProvider;
  usingOpenAI: boolean;
} {
  const providers = createProviders();
  return {
    embeddingProvider: providers.embeddingProvider,
    llmProvider: providers.llmProvider,
    usingOpenAI: providers.usingOpenAI,
  };
}

export function __test_only_ensureVectorLength(vector: number[], target: number): number[] {
  return ensureVectorLength(vector, target);
}

export function __test_only_normalizeRowsForPipeline(
  rows: Array<{ sourceSku: string; title: string; description?: string; brand?: string }>,
): NormalizedCatalogProduct[] {
  return normalizeRows(rows);
}

export function __test_only_partitionProductsBySample(
  products: NormalizedCatalogProduct[],
  storeId: string,
  sampleParts: number,
  samplePartIndex: number,
): {
  sampled: NormalizedCatalogProduct[];
  skipped: number;
} {
  return partitionProductsBySample(products, storeId, sampleParts, samplePartIndex);
}

export function __test_only_buildEscalationAttributeKeys(input: {
  contextAttributes: CategoryContext["attributes"];
  enrichment: ProductEnrichment;
  requiredMinConfidence: number;
  optionalMinConfidence: number;
}): string[] {
  const schema = buildEscalationAttributeSchema({
    context: {
      slug: "test",
      attributes: input.contextAttributes,
      description: "",
      confidenceScore: 0,
      top2Confidence: 0,
      margin: 0,
      autoDecision: "review",
      confidenceReasons: [],
      isFallbackCategory: false,
      contradictionCount: 0,
    },
    enrichment: input.enrichment,
    requiredMinConfidence: input.requiredMinConfidence,
    optionalMinConfidence: input.optionalMinConfidence,
  });
  return schema ? schema.attributes.map((attribute) => attribute.key).sort() : [];
}

export function __test_only_isEnrichmentImproved(
  current: ProductEnrichment,
  candidate: ProductEnrichment,
): boolean {
  return isEnrichmentImproved(current, candidate);
}

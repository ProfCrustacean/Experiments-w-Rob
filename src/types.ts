export type AttributeType = "enum" | "number" | "boolean" | "text";

export interface RawCatalogRow {
  sourceSku: string;
  title: string;
  description?: string;
  brand?: string;
  price?: number;
  availability?: string;
  url?: string;
  imageUrl?: string;
}

export interface NormalizedCatalogProduct extends RawCatalogRow {
  normalizedTitle: string;
  normalizedDescription: string;
  normalizedBrand: string;
  normalizedText: string;
}

export interface CategoryAttribute {
  key: string;
  label_pt: string;
  type: AttributeType;
  allowed_values?: string[];
  required: boolean;
}

export interface CategoryAttributeSchema {
  schema_version: "1.0";
  category_name_pt: string;
  attributes: CategoryAttribute[];
}

export interface CategoryDraft {
  name_pt: string;
  slug: string;
  description_pt: string;
  attributes_jsonb: CategoryAttributeSchema;
  synonyms: string[];
  sourceProductSkus: string[];
}

export interface ProductEnrichment {
  sourceSku: string;
  categorySlug: string;
  categoryConfidence: number;
  attributeValues: Record<string, string | number | boolean | null>;
  attributeConfidence: Record<string, number>;
  needsReview: boolean;
  uncertaintyReasons: string[];
}

export interface PersistedCategory {
  id: string;
  slug: string;
  name_pt: string;
  description_pt: string;
  attributes_jsonb: CategoryAttributeSchema;
}

export interface PersistedProduct {
  id: string;
  source_sku: string;
  title: string;
  category_id: string | null;
  category_confidence: number;
  needs_review: boolean;
  attribute_values_jsonb: Record<string, unknown>;
}

export interface PipelineRunSummary {
  runId: string;
  storeId: string;
  inputFileName: string;
  totalRows: number;
  uniqueProducts: number;
  categoryCount: number;
  needsReviewCount: number;
  qaReportPath: string;
  artifacts: RunArtifactSummary[];
  status: "completed_pending_review" | "failed";
}

export type RunArtifactFormat = "xlsx" | "csv" | "qa-csv";

export interface RunArtifactSummary {
  key: string;
  fileName: string;
  format: RunArtifactFormat;
  sizeBytes: number;
  expiresAt: string;
}

export type RunLogLevel = "debug" | "info" | "warn" | "error";

export interface PipelineRunLogRow {
  runId: string;
  seq: number;
  level: RunLogLevel;
  stage: string;
  event: string;
  message: string;
  payload: Record<string, unknown>;
  timestamp: string;
  expiresAt: string;
}

export interface CategoryCluster {
  key: string;
  candidateName: string;
  products: NormalizedCatalogProduct[];
  scoresBySku: Record<string, number>;
}

export interface CategoryProfileLLMOutput {
  name_pt: string;
  description_pt: string;
  synonyms: string[];
  attributes: CategoryAttribute[];
}

export interface AttributeExtractionLLMOutput {
  values: Record<string, string | number | boolean | null>;
  confidence: Record<string, number>;
}

export interface ProductAttributeExtractionInput {
  sourceSku: string;
  product: Pick<RawCatalogRow, "title" | "description" | "brand">;
}

export interface BatchAttributeExtractionInput {
  categoryName: string;
  categoryDescription: string;
  attributeSchema: CategoryAttributeSchema;
  products: ProductAttributeExtractionInput[];
}

export interface EmbeddingProvider {
  dimensions: number;
  embedMany(texts: string[]): Promise<number[][]>;
}

export interface LLMProvider {
  generateCategoryProfile(input: {
    candidateName: string;
    sampleProducts: Array<Pick<RawCatalogRow, "title" | "description" | "brand">>;
  }): Promise<CategoryProfileLLMOutput>;
  extractProductAttributes(input: {
    product: Pick<RawCatalogRow, "title" | "description" | "brand">;
    categoryName: string;
    categoryDescription: string;
    attributeSchema: CategoryAttributeSchema;
  }): Promise<AttributeExtractionLLMOutput>;
  extractProductAttributesBatch(
    input: BatchAttributeExtractionInput,
  ): Promise<Record<string, AttributeExtractionLLMOutput>>;
}

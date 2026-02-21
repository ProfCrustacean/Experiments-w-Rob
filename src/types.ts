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
  categoryTop2Confidence: number;
  categoryMargin: number;
  autoDecision: "auto" | "review";
  confidenceReasons: string[];
  isFallbackCategory: boolean;
  categoryContradictionCount: number;
  attributeValidationFailCount: number;
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

export type RunArtifactFormat = "xlsx" | "csv" | "qa-csv" | "confusion-csv";

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

export interface CategoryDisambiguationInput {
  product: Pick<RawCatalogRow, "title" | "description" | "brand">;
  candidates: Array<{
    slug: string;
    name_pt: string;
    description_pt: string;
  }>;
}

export interface CategoryDisambiguationOutput {
  categorySlug: string | null;
  confidence: number;
  reason: string;
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
  disambiguateCategory(
    input: CategoryDisambiguationInput,
  ): Promise<CategoryDisambiguationOutput>;
}

export interface TaxonomyCategory {
  slug: string;
  name_pt: string;
  description_pt: string;
  synonyms: string[];
  prototype_terms: string[];
  is_fallback: boolean;
  default_attributes: CategoryAttribute[];
}

export interface TaxonomyCategoryMatchRule {
  slug: string;
  include_any: string[];
  include_all: string[];
  exclude_any: string[];
  strong_exclude_any: string[];
  high_risk: boolean;
  out_of_scope?: boolean;
  auto_min_confidence?: number;
  auto_min_margin?: number;
}

export interface TaxonomyAttributePolicy {
  min?: number;
  max?: number;
  allow_negative?: boolean;
  pack_context_required?: boolean;
}

export interface TaxonomyAttributePolicyConfig {
  schema_version: string;
  attribute_policies: Record<string, TaxonomyAttributePolicy>;
  category_attribute_overrides: Record<string, Record<string, TaxonomyAttributePolicy>>;
}

export type SelfImproveLoopType = "canary" | "full";

export type SelfImproveBatchStatus =
  | "queued"
  | "running"
  | "completed"
  | "completed_with_failures"
  | "failed"
  | "cancelled";

export type SelfImproveRunStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "retried_succeeded"
  | "retried_failed";

export type SelfImproveAutoApplyPolicy = "if_gate_passes";

export interface SelfCorrectionContext {
  failureSummary: string;
  lastConfusionAlerts: Array<{
    category_slug: string;
    affected_count: number;
    low_margin_count: number;
    contradiction_count: number;
    fallback_count: number;
  }>;
  failedGateMetrics: string[];
  candidateFixes: string[];
}

export type SelfImproveProposalKind =
  | "rule_term_add"
  | "rule_term_remove"
  | "threshold_tune"
  | "taxonomy_merge"
  | "taxonomy_split"
  | "taxonomy_move";

export type SelfImproveProposalStatus = "proposed" | "applied" | "rejected" | "rolled_back";

export interface SelfImproveProposalPayload {
  target_slug: string;
  field: "include_any" | "exclude_any" | "strong_exclude_any" | "auto_min_confidence" | "auto_min_margin";
  action: "add" | "remove" | "set";
  value: string | number;
  reason: string;
}

export interface SelfImproveProposal {
  id: string;
  batchId: string | null;
  runId: string | null;
  proposalKind: SelfImproveProposalKind;
  status: SelfImproveProposalStatus;
  confidenceScore: number;
  expectedImpactScore: number;
  payload: SelfImproveProposalPayload;
  source: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface HarnessEvalResult {
  passed: boolean;
  metricScores: Record<string, number>;
  failedMetrics: string[];
  baselineRunId: string | null;
  candidateRunId: string | null;
}

export interface AppliedChangeRecord {
  id: string;
  proposalId: string;
  proposalKind: SelfImproveProposalKind;
  status: "applied" | "rolled_back";
  versionBefore: string;
  versionAfter: string;
  appliedAt: string;
  rollbackToken: string;
  metadata: Record<string, unknown>;
}

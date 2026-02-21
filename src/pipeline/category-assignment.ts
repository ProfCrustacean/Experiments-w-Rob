import pLimit from "p-limit";
import type {
  CategoryDisambiguationOutput,
  EmbeddingProvider,
  LLMProvider,
  NormalizedCatalogProduct,
  TaxonomyCategory,
} from "../types.js";
import { chunk } from "../utils/collections.js";
import { detectFormat, detectPackCount, detectRuling, normalizeText } from "../utils/text.js";
import { loadTaxonomy } from "../taxonomy/load.js";

export interface CategoryAssignment {
  sourceSku: string;
  categorySlug: string;
  categoryTop2Slug: string | null;
  categoryConfidence: number;
  categoryTop2Confidence: number;
  categoryMargin: number;
  autoDecision: "auto" | "review";
  confidenceReasons: string[];
  isFallbackCategory: boolean;
  categoryContradictionCount: number;
  lexicalScore: number;
  semanticScore: number;
  attributeCompatibilityScore: number;
}

export interface CategoryAssignmentOutput {
  assignmentsBySku: Map<string, CategoryAssignment>;
  confidenceHistogram: Record<string, number>;
  topConfusionAlerts: Array<{
    category_slug: string;
    affected_count: number;
    low_margin_count: number;
    contradiction_count: number;
    fallback_count: number;
  }>;
}

interface CandidateScore {
  category: TaxonomyCategory;
  score: number;
  lexical: number;
  semantic: number;
  compatibility: number;
  contradictionCount: number;
  lexicalEligible: boolean;
  includeHits: number;
  excludeHits: number;
  strongExcluded: boolean;
}

interface AssignCategoriesInput {
  products: NormalizedCatalogProduct[];
  embeddingProvider: EmbeddingProvider;
  llmProvider: LLMProvider | null;
  autoMinConfidence: number;
  autoMinMargin: number;
  highRiskExtraConfidence: number;
  llmConcurrency: number;
  embeddingBatchSize: number;
  embeddingConcurrency: number;
}

function dot(a: number[], b: number[]): number {
  let sum = 0;
  const max = Math.min(a.length, b.length);
  for (let index = 0; index < max; index += 1) {
    sum += a[index] * b[index];
  }
  return sum;
}

function magnitude(vector: number[]): number {
  let sum = 0;
  for (const value of vector) {
    sum += value * value;
  }
  return Math.sqrt(sum);
}

function cosineSimilarity(a: number[], b: number[]): number {
  const denominator = magnitude(a) * magnitude(b);
  if (denominator === 0) {
    return 0;
  }
  return dot(a, b) / denominator;
}

function normalizeSimilarity(similarity: number): number {
  const normalized = (similarity + 1) / 2;
  return Math.max(0, Math.min(1, normalized));
}

function clampScore(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, Math.min(1, value));
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function hasTerm(normalizedText: string, term: string): boolean {
  const normalizedTerm = normalizeText(term);
  if (!normalizedTerm || normalizedTerm.length < 2) {
    return false;
  }

  const pattern = normalizedTerm
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => escapeRegex(part))
    .join("\\s+");

  const regex = new RegExp(`(?:^|\\b)${pattern}(?:\\b|$)`);
  return regex.test(normalizedText);
}

function countTermHits(normalizedText: string, normalizedTitle: string, terms: string[]): number {
  let hits = 0;
  for (const term of terms) {
    const normalizedTerm = normalizeText(term);
    if (!normalizedTerm || normalizedTerm.length < 2) {
      continue;
    }

    if (hasTerm(normalizedText, normalizedTerm)) {
      hits += hasTerm(normalizedTitle, normalizedTerm) ? 2 : 1;
    }
  }
  return hits;
}

function estimateAttributeCompatibility(product: NormalizedCatalogProduct, category: TaxonomyCategory): number {
  if (category.default_attributes.length === 0) {
    return 0.4;
  }

  const text = `${product.normalizedTitle} ${product.normalizedDescription}`;
  let matched = 0;

  for (const attribute of category.default_attributes) {
    switch (attribute.key) {
      case "item_subtype": {
        const subtypeMatch = (attribute.allowed_values ?? []).some((value) => hasTerm(text, value));
        matched += subtypeMatch ? 1 : 0;
        break;
      }
      case "format":
        matched += detectFormat(text) ? 1 : 0;
        break;
      case "ruling":
        matched += detectRuling(text) ? 1 : 0;
        break;
      case "sheet_count":
        matched += /\b\d{2,4}\s*(folhas|fls)\b/.test(text) ? 1 : 0;
        break;
      case "pack_count":
        matched += detectPackCount(text) !== null ? 1 : 0;
        break;
      case "ink_type":
        matched += /(gel|esferografica|esferografico|roller)/.test(text) ? 1 : 0;
        break;
      case "hardness":
        matched += /\b(hb|2b|b|h)\b/.test(text) ? 1 : 0;
        break;
      case "point_size_mm":
        matched += /\b\d(?:[.,]\d)?\s*mm\b/.test(text) ? 1 : 0;
        break;
      case "glue_type":
        matched += /(cola\s+bastao|cola\s+liquida|cola\s+líquida|stick)/.test(text) ? 1 : 0;
        break;
      case "tape_type":
        matched += /(fita|dupla face|washi|masking)/.test(text) ? 1 : 0;
        break;
      case "volume_ml":
        matched += /\b\d{1,4}\s*ml\b/.test(text) ? 1 : 0;
        break;
      case "length_cm":
        matched += /\b\d{1,3}(?:[.,]\d+)?\s*cm\b/.test(text) ? 1 : 0;
        break;
      case "tip_safety":
        matched += /(ponta redonda|seguranca|segurança)/.test(text) ? 1 : 0;
        break;
      case "has_wheels":
        matched += /(rodas|com rodas|sem rodas|trolley)/.test(text) ? 1 : 0;
        break;
      case "capacity_l":
        matched += /\b\d{1,2}(?:[.,]\d+)?\s*(l|litros?)\b/.test(text) ? 1 : 0;
        break;
      case "compartment_count":
        matched += /\b(duplo|triplo|\d\s*compartimentos?)\b/.test(text) ? 1 : 0;
        break;
      case "paint_type":
        matched += /(guache|aquarela|acrilica|acrílica|tempera|têmpera)/.test(text) ? 1 : 0;
        break;
      case "brush_type":
        matched += /(pincel\s*chato|pincel\s*redondo|esponja)/.test(text) ? 1 : 0;
        break;
      case "weight_gsm":
        matched += /\b\d{2,3}\s*(g\/m2|g\/m²|g)\b/.test(text) ? 1 : 0;
        break;
      case "target_age":
        matched += /(infantil|juvenil|kids|teen)/.test(text) ? 1 : 0;
        break;
      case "color_set":
        matched += /(\d+\s*cores|colorido|multicor)/.test(text) ? 1 : 0;
        break;
      default:
        matched += 0;
        break;
    }
  }

  return matched / category.default_attributes.length;
}

function detectCategoryContradictions(categorySlug: string, product: NormalizedCatalogProduct): number {
  const text = `${product.normalizedTitle} ${product.normalizedDescription}`;
  let contradictions = 0;

  if (
    categorySlug === "cadernos_blocos" &&
    /(agrafador|agrafos|etiquetas|mochila|tesoura|regua|fita adesiva)/.test(text) &&
    !/(caderno|bloco|flashcards|recarga)/.test(text)
  ) {
    contradictions += 1;
  }

  if (
    categorySlug === "escrita" &&
    /(pasta arquivo|classificador|resma|mochila|estojo|tesoura)/.test(text) &&
    !/(caneta|lapis|lápis|marcador|borracha|afiador|corretor)/.test(text)
  ) {
    contradictions += 1;
  }

  if (
    categorySlug === "organizacao_arquivo" &&
    /(lapis de cor|caneta gel|tesoura escolar|mochila)/.test(text) &&
    !/(pasta|arquivo|etiquet|agrafador|agrafos|clips|post-it|aderente)/.test(text)
  ) {
    contradictions += 1;
  }

  if (
    categorySlug === "geometria_corte" &&
    /(caneta|lapis|cola|mochila|resma)/.test(text) &&
    !/(regua|régua|esquadro|transferidor|tesoura|compasso)/.test(text)
  ) {
    contradictions += 1;
  }

  if (
    categorySlug === "transporte_escolar" &&
    !/(mochila|backpack|trolley|estojo|penal|porta lapis|porta lápis)/.test(text)
  ) {
    contradictions += 1;
  }

  if (
    categorySlug === "cola_adesivos" &&
    !/(cola|bastao|bastão|liquida|líquida|fita|adesiva|dupla face|rollafix)/.test(text)
  ) {
    contradictions += 1;
  }

  if (
    categorySlug === "papel" &&
    /(caneta|lapis|lápis|agrafador|mochila|estojo)/.test(text) &&
    !/(resma|papel|folhas|gramagem|g\/m2|g\/m²)/.test(text)
  ) {
    contradictions += 1;
  }

  return contradictions;
}

function buildConfidenceHistogram(assignments: Iterable<CategoryAssignment>): Record<string, number> {
  const histogram: Record<string, number> = {
    "0.0-0.2": 0,
    "0.2-0.4": 0,
    "0.4-0.6": 0,
    "0.6-0.8": 0,
    "0.8-1.0": 0,
  };

  for (const assignment of assignments) {
    const confidence = assignment.categoryConfidence;
    if (confidence < 0.2) {
      histogram["0.0-0.2"] += 1;
    } else if (confidence < 0.4) {
      histogram["0.2-0.4"] += 1;
    } else if (confidence < 0.6) {
      histogram["0.4-0.6"] += 1;
    } else if (confidence < 0.8) {
      histogram["0.6-0.8"] += 1;
    } else {
      histogram["0.8-1.0"] += 1;
    }
  }

  return histogram;
}

function shouldUseLlmDisambiguation(input: {
  top1: CandidateScore;
  top2: CandidateScore | null;
  requiredConfidence: number;
  requiredMargin: number;
}): boolean {
  if (!input.top2) {
    return false;
  }

  const margin = input.top1.score - input.top2.score;
  const closeScores = margin < Math.max(input.requiredMargin + 0.03, 0.1);
  const weakLexical = input.top1.lexical < 0.55;
  const nearConfidenceBoundary = input.top1.score < input.requiredConfidence + 0.03;
  return closeScores || (weakLexical && nearConfidenceBoundary);
}

function resolveCategoryThresholds(input: {
  rule:
    | {
        high_risk?: boolean;
        auto_min_confidence?: number;
        auto_min_margin?: number;
      }
    | undefined;
  autoMinConfidence: number;
  autoMinMargin: number;
  highRiskExtraConfidence: number;
}): {
  requiredConfidence: number;
  requiredMargin: number;
} {
  const hasConfidenceOverride = typeof input.rule?.auto_min_confidence === "number";
  const hasMarginOverride = typeof input.rule?.auto_min_margin === "number";

  const requiredConfidence = hasConfidenceOverride
    ? clampScore(input.rule?.auto_min_confidence ?? input.autoMinConfidence)
    : clampScore(
        input.autoMinConfidence + (input.rule?.high_risk ? input.highRiskExtraConfidence : 0),
      );
  const requiredMargin = hasMarginOverride
    ? clampScore(input.rule?.auto_min_margin ?? input.autoMinMargin)
    : clampScore(input.autoMinMargin);

  return {
    requiredConfidence,
    requiredMargin,
  };
}

function buildCategoryPrototypeText(category: TaxonomyCategory): string {
  return [
    category.name_pt,
    category.description_pt,
    ...category.synonyms,
    ...category.prototype_terms,
  ]
    .join(" ")
    .trim();
}

async function embedTextsWithBatches(input: {
  texts: string[];
  provider: EmbeddingProvider;
  batchSize: number;
  concurrency: number;
}): Promise<number[][]> {
  if (input.texts.length === 0) {
    return [];
  }

  const groups = chunk(input.texts, Math.max(1, input.batchSize));
  const limiter = pLimit(Math.max(1, Math.floor(input.concurrency)));
  const output = new Array<number[]>(input.texts.length);

  await Promise.all(
    groups.map((group, groupIndex) =>
      limiter(async () => {
        const start = groupIndex * Math.max(1, input.batchSize);
        const vectors = await input.provider.embedMany(group);
        for (let index = 0; index < vectors.length; index += 1) {
          output[start + index] = vectors[index];
        }
      }),
    ),
  );

  return output;
}

function toDisambiguationCandidates(candidates: CandidateScore[]): Array<{
  slug: string;
  name_pt: string;
  description_pt: string;
}> {
  return candidates.map((candidate) => ({
    slug: candidate.category.slug,
    name_pt: candidate.category.name_pt,
    description_pt: candidate.category.description_pt,
  }));
}

function applyLlmChoice(
  ranked: CandidateScore[],
  llmOutput: CategoryDisambiguationOutput,
): CandidateScore[] {
  if (!llmOutput.categorySlug) {
    return ranked;
  }

  const chosenIndex = ranked.findIndex((candidate) => candidate.category.slug === llmOutput.categorySlug);
  if (chosenIndex < 0) {
    return ranked;
  }

  const chosen = ranked[chosenIndex];
  const boostedScore = clampScore(chosen.score + Math.min(0.04, Math.max(0, llmOutput.confidence - 0.5) * 0.08));
  const updatedChosen: CandidateScore = {
    ...chosen,
    score: boostedScore,
  };

  const rest = ranked.filter((_, index) => index !== chosenIndex);
  return [updatedChosen, ...rest].sort((left, right) => right.score - left.score);
}

function pickFallbackRescueCandidate(ranked: CandidateScore[]): CandidateScore | null {
  const top1 = ranked[0];
  if (!top1 || !top1.category.is_fallback) {
    return null;
  }

  for (const candidate of ranked) {
    if (candidate.category.is_fallback) {
      continue;
    }

    const hasStrongLexicalEvidence =
      candidate.includeHits >= 2 || (candidate.includeHits >= 1 && candidate.lexical >= 0.6);
    const hasStrongSemanticEvidence = candidate.semantic >= 0.74 && candidate.compatibility >= 0.3;
    if (
      (hasStrongLexicalEvidence || hasStrongSemanticEvidence) &&
      candidate.lexicalEligible &&
      candidate.contradictionCount === 0 &&
      candidate.excludeHits === 0 &&
      !candidate.strongExcluded &&
      candidate.score >= 0.32
    ) {
      return candidate;
    }
  }

  return null;
}

export async function assignCategoriesForProducts(
  input: AssignCategoriesInput,
): Promise<CategoryAssignmentOutput> {
  const taxonomy = loadTaxonomy();
  const categoryTexts = taxonomy.categories.map((category) => buildCategoryPrototypeText(category));
  const categoryVectors = await input.embeddingProvider.embedMany(categoryTexts);
  const categoryVectorBySlug = new Map<string, number[]>();

  for (let index = 0; index < taxonomy.categories.length; index += 1) {
    categoryVectorBySlug.set(taxonomy.categories[index].slug, categoryVectors[index]);
  }

  const productVectors = await embedTextsWithBatches({
    texts: input.products.map(
      (product) => `${product.normalizedTitle} ${product.normalizedDescription} ${product.normalizedBrand}`,
    ),
    provider: input.embeddingProvider,
    batchSize: input.embeddingBatchSize,
    concurrency: input.embeddingConcurrency,
  });

  const llmLimiter = pLimit(Math.max(1, Math.floor(input.llmConcurrency)));
  const assignmentEntries = await Promise.all(
    input.products.map((product, index) =>
      llmLimiter(async () => {
        const productVector = productVectors[index];
        const normalizedText = `${product.normalizedTitle} ${product.normalizedDescription} ${product.normalizedBrand}`;
        const candidateScores: CandidateScore[] = [];

        for (const category of taxonomy.categories) {
          const rule = taxonomy.rulesBySlug.get(category.slug);

          const includeTerms = rule?.include_any ?? [];
          const includeAllTerms = rule?.include_all ?? [];
          const excludeTerms = rule?.exclude_any ?? [];
          const strongExcludeTerms = rule?.strong_exclude_any ?? [];

          const includeHits = countTermHits(normalizedText, product.normalizedTitle, includeTerms);
          const includeAllMisses = includeAllTerms.filter((term) => !hasTerm(normalizedText, term)).length;
          const excludeHits = excludeTerms.filter((term) => hasTerm(normalizedText, term)).length;
          const strongExcludeHits = strongExcludeTerms.filter((term) => hasTerm(normalizedText, term)).length;

          const includeAnySatisfied = includeTerms.length === 0 || includeHits > 0;
          const includeAllSatisfied = includeAllTerms.length === 0 || includeAllMisses === 0;
          const lexicalEligible = includeAnySatisfied && includeAllSatisfied;

          const lexicalDenominator = includeTerms.length === 0 ? 2 : Math.max(2, Math.ceil(includeTerms.length / 2));
          let lexicalScore = includeTerms.length === 0 ? 0.35 : Math.min(1, includeHits / lexicalDenominator);

          if (!includeAnySatisfied) {
            lexicalScore *= 0.45;
          }
          if (!includeAllSatisfied) {
            lexicalScore *= 0.45;
          }
          lexicalScore = clampScore(lexicalScore - Math.min(0.3, excludeHits * 0.08));

          const categoryVector = categoryVectorBySlug.get(category.slug);
          const semanticRaw = categoryVector ? cosineSimilarity(productVector, categoryVector) : 0;
          const semanticScore = normalizeSimilarity(semanticRaw);
          const compatibilityScore = estimateAttributeCompatibility(product, category);
          const contradictionCount = detectCategoryContradictions(category.slug, product);

          const strongExcluded = strongExcludeHits > 0;
          let score = 0.45 * lexicalScore + 0.35 * semanticScore + 0.2 * compatibilityScore;
          score -= contradictionCount * 0.18;
          score -= Math.min(0.24, excludeHits * 0.12);

          if (category.is_fallback) {
            score = Math.max(score * 0.35, 0.12 + semanticScore * 0.08);
            lexicalScore = Math.min(lexicalScore, 0.06);
          }

          if (strongExcluded) {
            score = 0;
          }

          candidateScores.push({
            category,
            score: clampScore(score),
            lexical: lexicalScore,
            semantic: semanticScore,
            compatibility: compatibilityScore,
            contradictionCount,
            lexicalEligible,
            includeHits,
            excludeHits,
            strongExcluded,
          });
        }

        const nonFallbackCandidates = candidateScores.filter((candidate) => !candidate.category.is_fallback);
        const lexicalCandidates = nonFallbackCandidates
          .filter((candidate) => candidate.lexicalEligible && !candidate.strongExcluded)
          .sort((left, right) => right.score - left.score);
        const semanticCandidates = nonFallbackCandidates
          .filter((candidate) => !candidate.strongExcluded)
          .sort((left, right) => right.score - left.score);

        const fallbackCandidate =
          candidateScores.find((candidate) => candidate.category.slug === taxonomy.fallbackCategory.slug) ??
          candidateScores[0];

        let ranked = lexicalCandidates;
        if (ranked.length === 0) {
          ranked = semanticCandidates.slice(0, 5);
        }

        if (ranked.length === 0) {
          ranked = [fallbackCandidate];
        } else {
          ranked = [...ranked, fallbackCandidate].sort((left, right) => right.score - left.score);
        }

        const llmCandidates = ranked.filter((candidate) => !candidate.category.is_fallback).slice(0, 3);
        const top1BeforeLlm = llmCandidates[0] ?? ranked[0];
        const top2BeforeLlm = llmCandidates[1] ?? ranked[1] ?? null;
        const top1PreRule = taxonomy.rulesBySlug.get(top1BeforeLlm.category.slug);
        const preThresholds = resolveCategoryThresholds({
          rule: top1PreRule,
          autoMinConfidence: input.autoMinConfidence,
          autoMinMargin: input.autoMinMargin,
          highRiskExtraConfidence: input.highRiskExtraConfidence,
        });

        const useLlm =
          input.llmProvider !== null &&
          lexicalCandidates.length > 0 &&
          llmCandidates.length >= 2 &&
          shouldUseLlmDisambiguation({
            top1: top1BeforeLlm,
            top2: top2BeforeLlm,
            requiredConfidence: preThresholds.requiredConfidence,
            requiredMargin: preThresholds.requiredMargin,
          });

        let llmReason = "";
        if (useLlm && input.llmProvider) {
          const llmOutput = await input.llmProvider.disambiguateCategory({
            product: {
              title: product.title,
              description: product.description,
              brand: product.brand,
            },
            candidates: toDisambiguationCandidates(llmCandidates),
          });
          if (llmOutput.categorySlug) {
            ranked = applyLlmChoice(ranked, llmOutput);
            llmReason = llmOutput.reason || "llm_disambiguation";
          }
        }

        const rescuedCandidate = pickFallbackRescueCandidate(ranked);
        const top1 = rescuedCandidate ?? ranked[0] ?? fallbackCandidate;
        const reRankedTop = [top1, ...ranked.filter((candidate) => candidate.category.slug !== top1.category.slug)];
        const top2 = reRankedTop[1] ?? null;
        const margin = clampScore(top1.score - (top2?.score ?? 0));
        const fallbackRescueApplied = Boolean(rescuedCandidate);

        const rule = taxonomy.rulesBySlug.get(top1.category.slug);
        const thresholds = resolveCategoryThresholds({
          rule,
          autoMinConfidence: input.autoMinConfidence,
          autoMinMargin: input.autoMinMargin,
          highRiskExtraConfidence: input.highRiskExtraConfidence,
        });

        const requiredConfidence = thresholds.requiredConfidence;
        const requiredMargin = thresholds.requiredMargin;
        const isOutOfScopeCategory = Boolean(rule?.out_of_scope);

        const confidenceReasons: string[] = ["family_assignment"];
        if (top1.lexical >= 0.6) {
          confidenceReasons.push("strong_lexical_match");
        }
        if (top1.semantic >= 0.75) {
          confidenceReasons.push("strong_semantic_match");
        }
        if (top1.compatibility >= 0.5) {
          confidenceReasons.push("attribute_compatible");
        }
        if (top1.contradictionCount > 0) {
          confidenceReasons.push("category_contradiction");
        }
        if (top1.score < requiredConfidence) {
          confidenceReasons.push("below_auto_confidence");
        }
        if (margin < requiredMargin) {
          confidenceReasons.push("low_margin");
        }
        if (top1.category.is_fallback) {
          confidenceReasons.push("generic_or_fallback_category");
        }
        if (isOutOfScopeCategory) {
          confidenceReasons.push("out_of_scope_category");
        }
        if (fallbackRescueApplied) {
          confidenceReasons.push("fallback_rescue_applied");
        }
        if (llmReason) {
          confidenceReasons.push(llmReason);
        }

        let autoDecision: "auto" | "review" =
          top1.score >= requiredConfidence &&
          margin >= requiredMargin &&
          top1.contradictionCount === 0 &&
          !isOutOfScopeCategory &&
          !fallbackRescueApplied
            ? "auto"
            : "review";

        if (top1.category.slug === "outros_escolares") {
          const strictConfidence = Math.max(requiredConfidence, 0.86);
          const strictMargin = Math.max(requiredMargin, 0.18);
          autoDecision =
            top1.score >= strictConfidence &&
            margin >= strictMargin &&
            top1.contradictionCount === 0 &&
            !fallbackRescueApplied
              ? "auto"
              : "review";
        }

        if (top1.category.slug === "fora_escopo_escolar") {
          autoDecision = "review";
        }

        const assignment: CategoryAssignment = {
          sourceSku: product.sourceSku,
          categorySlug: top1.category.slug,
          categoryTop2Slug: top2?.category.slug ?? null,
          categoryConfidence: top1.score,
          categoryTop2Confidence: top2?.score ?? 0,
          categoryMargin: margin,
          autoDecision,
          confidenceReasons,
          isFallbackCategory: Boolean(top1.category.is_fallback),
          categoryContradictionCount: top1.contradictionCount,
          lexicalScore: top1.lexical,
          semanticScore: top1.semantic,
          attributeCompatibilityScore: top1.compatibility,
        };

        return assignment;
      }),
    ),
  );

  const assignmentsBySku = new Map<string, CategoryAssignment>();
  const alertStats = new Map<
    string,
    {
      affected_count: number;
      low_margin_count: number;
      contradiction_count: number;
      fallback_count: number;
    }
  >();

  for (const assignment of assignmentEntries) {
    assignmentsBySku.set(assignment.sourceSku, assignment);

    const existing = alertStats.get(assignment.categorySlug) ?? {
      affected_count: 0,
      low_margin_count: 0,
      contradiction_count: 0,
      fallback_count: 0,
    };

    existing.affected_count += 1;
    existing.low_margin_count += assignment.confidenceReasons.includes("low_margin") ? 1 : 0;
    existing.contradiction_count += assignment.categoryContradictionCount;
    existing.fallback_count += assignment.isFallbackCategory ? 1 : 0;

    alertStats.set(assignment.categorySlug, existing);
  }

  const confidenceHistogram = buildConfidenceHistogram(assignmentsBySku.values());
  const topConfusionAlerts = [...alertStats.entries()]
    .map(([categorySlug, stats]) => ({
      category_slug: categorySlug,
      ...stats,
    }))
    .sort(
      (left, right) =>
        right.contradiction_count + right.low_margin_count - (left.contradiction_count + left.low_margin_count),
    )
    .slice(0, 10);

  return {
    assignmentsBySku,
    confidenceHistogram,
    topConfusionAlerts,
  };
}

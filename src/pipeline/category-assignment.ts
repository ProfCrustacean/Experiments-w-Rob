import pLimit from "p-limit";
import type {
  CategoryDisambiguationOutput,
  EmbeddingProvider,
  LLMProvider,
  NormalizedCatalogProduct,
  TaxonomyCategory,
} from "../types.js";
import { normalizeText, detectFormat, detectRuling, detectPackCount } from "../utils/text.js";
import { loadTaxonomy } from "../taxonomy/load.js";

export interface CategoryAssignment {
  sourceSku: string;
  categorySlug: string;
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
}

const PACK_CONTEXT_REGEX = /(pack|caixa|conjunto|kit|unid|unidades|pcs|pecas|x\s*\d+)/;
const SHEET_CONTEXT_REGEX = /(folhas|fls|resma|caderno|bloco|recarga)/;

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

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function hasTerm(normalizedText: string, term: string): boolean {
  const normalizedTerm = normalizeText(term);
  if (!normalizedTerm) {
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
    if (!normalizedTerm) {
      continue;
    }

    if (normalizedText.includes(normalizedTerm)) {
      hits += normalizedTitle.includes(normalizedTerm) ? 2 : 1;
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
      case "hardness":
        matched += /\b(hb|2b|b|h)\b/.test(text) ? 1 : 0;
        break;
      case "ink_type":
        matched += /(gel|esferografica|esferografico|roller)/.test(text) ? 1 : 0;
        break;
      case "point_size_mm":
        matched += /\b\d(?:[.,]\d)?\s*mm\b/.test(text) ? 1 : 0;
        break;
      case "glue_type":
        matched += /(cola\s+bastao|cola\s+liquida|cola\s+líquida|stick)/.test(text) ? 1 : 0;
        break;
      case "volume_ml":
        matched += /\b\d{1,4}\s*ml\b/.test(text) ? 1 : 0;
        break;
      case "length_cm":
        matched += /\b\d{1,3}(?:[.,]\d+)?\s*cm\b/.test(text) ? 1 : 0;
        break;
      case "capacity_l":
        matched += /\b\d{1,2}(?:[.,]\d+)?\s*(l|litros?)\b/.test(text) ? 1 : 0;
        break;
      case "has_wheels":
        matched += /(rodas|com rodas|sem rodas)/.test(text) ? 1 : 0;
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

  if (categorySlug === "lapis-cor" && /(afia|afiador|agrafador|agrafos|post-it|notas aderentes|etiquetas|resma|bloco)/.test(text)) {
    contradictions += 1;
  }

  if (categorySlug.startsWith("caderno-") && /(agrafador|agrafos|etiquetas|afia|resma)/.test(text)) {
    contradictions += 1;
  }

  if (categorySlug.startsWith("caneta") && /(lapis|lápis|afia|agrafador|agrafos)/.test(text)) {
    contradictions += 1;
  }

  if (categorySlug === "papel-a4" && /(caderno\s+espiral|brochura)/.test(text)) {
    contradictions += 1;
  }

  if (categorySlug === "bloco-notas-aderentes" && !/(aderente|post-it|notas)/.test(text)) {
    contradictions += 1;
  }

  if ((categorySlug === "cola-bastao" || categorySlug === "cola-liquida") && !/cola/.test(text)) {
    contradictions += 1;
  }

  if (!PACK_CONTEXT_REGEX.test(text) && SHEET_CONTEXT_REGEX.test(text) && detectPackCount(text) !== null) {
    contradictions += 1;
  }

  return contradictions;
}

function clampScore(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, Math.min(1, value));
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
  autoMinConfidence: number;
  autoMinMargin: number;
}): boolean {
  if (!input.top2) {
    return false;
  }

  const margin = input.top1.score - input.top2.score;
  const closeScores = margin < Math.max(input.autoMinMargin + 0.04, 0.14);
  const lowConfidence = input.top1.score < input.autoMinConfidence + 0.08;
  return closeScores || lowConfidence;
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
  // LLM is a tie-breaker only; keep confidence grounded in lexical/semantic evidence.
  const boostedScore = clampScore(chosen.score + Math.min(0.04, Math.max(0, llmOutput.confidence - 0.5) * 0.08));
  const updatedChosen: CandidateScore = {
    ...chosen,
    score: boostedScore,
  };

  const rest = ranked.filter((_, index) => index !== chosenIndex);
  return [updatedChosen, ...rest].sort((left, right) => right.score - left.score);
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

  const productVectors = await input.embeddingProvider.embedMany(
    input.products.map((product) => `${product.normalizedTitle} ${product.normalizedDescription} ${product.normalizedBrand}`),
  );

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

          const lexicalBase = includeTerms.length === 0 ? 0 : includeHits / (includeTerms.length * 2);
          let lexicalScore = clampScore(lexicalBase);
          if (includeAllTerms.length > 0 && includeAllMisses > 0) {
            lexicalScore *= 0.45;
          }

          const categoryVector = categoryVectorBySlug.get(category.slug);
          const semanticRaw = categoryVector ? cosineSimilarity(productVector, categoryVector) : 0;
          const semanticScore = normalizeSimilarity(semanticRaw);
          const compatibilityScore = estimateAttributeCompatibility(product, category);
          const contradictionCount = detectCategoryContradictions(category.slug, product);

          const strongExcluded = strongExcludeHits > 0;
          if (category.is_fallback) {
            lexicalScore = 0.05;
          }

          let score = 0.45 * lexicalScore + 0.35 * semanticScore + 0.2 * compatibilityScore;
          score -= Math.min(0.25, excludeHits * 0.12);
          score -= contradictionCount * 0.18;
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
            excludeHits,
            strongExcluded,
          });
        }

        const nonFallbackCandidates = candidateScores.filter((candidate) => !candidate.category.is_fallback);
        const lexicalCandidates = nonFallbackCandidates
          .filter((candidate) => candidate.lexicalEligible && !candidate.strongExcluded)
          .sort((left, right) => right.score - left.score);

        const fallbackCandidate =
          candidateScores.find((candidate) => candidate.category.slug === taxonomy.fallbackCategory.slug) ??
          candidateScores[0];

        let ranked = lexicalCandidates;
        if (ranked.length === 0) {
          ranked = [fallbackCandidate];
        } else {
          ranked = [...ranked, fallbackCandidate].sort((left, right) => right.score - left.score);
        }

        const llmCandidates = ranked.slice(0, 3);
        const top1BeforeLlm = llmCandidates[0];
        const top2BeforeLlm = llmCandidates[1] ?? null;

        const useLlm =
          input.llmProvider !== null &&
          shouldUseLlmDisambiguation({
            top1: top1BeforeLlm,
            top2: top2BeforeLlm,
            autoMinConfidence: input.autoMinConfidence,
            autoMinMargin: input.autoMinMargin,
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

        const top1 = ranked[0] ?? fallbackCandidate;
        const top2 = ranked[1] ?? null;
        const margin = clampScore(top1.score - (top2?.score ?? 0));
        const mixedInkSignals =
          /gel/.test(normalizedText) &&
          /(esferografica|esferográfico|esferografico|ballpoint)/.test(normalizedText) &&
          (top1.category.slug.startsWith("caneta-") || top2?.category.slug.startsWith("caneta-") === true);

        const rule = taxonomy.rulesBySlug.get(top1.category.slug);
        const requiredConfidence =
          input.autoMinConfidence + (rule?.high_risk ? input.highRiskExtraConfidence : 0);

        const isGenericCategory =
          top1.category.is_fallback ||
          /geral|diverso/.test(top1.category.slug) ||
          /geral|diverso/.test(normalizeText(top1.category.name_pt));

        const confidenceReasons: string[] = [];
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
        if (margin < input.autoMinMargin) {
          confidenceReasons.push("low_margin");
        }
        if (isGenericCategory) {
          confidenceReasons.push("generic_or_fallback_category");
        }
        if (mixedInkSignals) {
          confidenceReasons.push("mixed_ink_signals");
        }
        if (llmReason) {
          confidenceReasons.push(llmReason);
        }

        const autoDecision: "auto" | "review" =
          top1.score >= requiredConfidence &&
          margin >= input.autoMinMargin &&
          top1.contradictionCount === 0 &&
          !mixedInkSignals &&
          !isGenericCategory
            ? "auto"
            : "review";

        const assignment: CategoryAssignment = {
          sourceSku: product.sourceSku,
          categorySlug: top1.category.slug,
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
    existing.low_margin_count += assignment.categoryMargin < input.autoMinMargin ? 1 : 0;
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

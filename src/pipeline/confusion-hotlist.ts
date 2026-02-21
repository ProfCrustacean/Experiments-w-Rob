import type { NormalizedCatalogProduct, TaxonomyCategoryMatchRule } from "../types.js";
import type { CategoryAssignment } from "./category-assignment.js";
import { loadTaxonomy } from "../taxonomy/load.js";
import { normalizeText, tokenize } from "../utils/text.js";

export interface ConfusionHotlistRow {
  category_a: string;
  category_b: string;
  affected_count: number;
  low_margin_count: number;
  contradiction_count: number;
  sample_skus: string;
  sample_titles: string;
  top_tokens: string;
  suggested_include_a: string;
  suggested_exclude_a: string;
  suggested_include_b: string;
  suggested_exclude_b: string;
}

export interface ConfusionHotlistResult {
  rows: ConfusionHotlistRow[];
  csvContent: string;
}

interface ConfusionBucket {
  categoryA: string;
  categoryB: string;
  assignments: Array<{
    assignment: CategoryAssignment;
    product: NormalizedCatalogProduct;
  }>;
  tokenCounts: Map<string, number>;
  lowMarginCount: number;
  contradictionCount: number;
}

function shouldIncludeForConfusion(assignment: CategoryAssignment): boolean {
  if (!assignment.categoryTop2Slug || assignment.categoryTop2Slug === assignment.categorySlug) {
    return false;
  }

  if (assignment.autoDecision === "review") {
    return true;
  }

  if (assignment.categoryMargin < 0.12) {
    return true;
  }

  return assignment.categoryContradictionCount > 0;
}

function sortPair(a: string, b: string): [string, string] {
  return a <= b ? [a, b] : [b, a];
}

function toCsvValue(value: string): string {
  if (/[",\n]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function toCsv(rows: ConfusionHotlistRow[]): string {
  const headers: Array<keyof ConfusionHotlistRow> = [
    "category_a",
    "category_b",
    "affected_count",
    "low_margin_count",
    "contradiction_count",
    "sample_skus",
    "sample_titles",
    "top_tokens",
    "suggested_include_a",
    "suggested_exclude_a",
    "suggested_include_b",
    "suggested_exclude_b",
  ];

  const lines = rows.map((row) =>
    headers
      .map((header) => toCsvValue(String(row[header] ?? "")))
      .join(","),
  );

  return [headers.join(","), ...lines].join("\n");
}

function normalizeTerms(terms: string[]): Set<string> {
  const output = new Set<string>();
  for (const term of terms) {
    for (const token of tokenize(term)) {
      if (token.length >= 3) {
        output.add(token);
      }
    }
  }
  return output;
}

function getDescriptorTokens(categorySlug: string): Set<string> {
  const taxonomy = loadTaxonomy();
  const category = taxonomy.categoriesBySlug.get(categorySlug);
  if (!category) {
    return new Set<string>();
  }

  return normalizeTerms([
    category.name_pt,
    category.description_pt,
    ...category.synonyms,
    ...category.prototype_terms,
  ]);
}

function getRule(slug: string): TaxonomyCategoryMatchRule {
  const taxonomy = loadTaxonomy();
  const rule = taxonomy.rulesBySlug.get(slug);
  if (rule) {
    return rule;
  }

  return {
    slug,
    include_any: [],
    include_all: [],
    exclude_any: [],
    strong_exclude_any: [],
    high_risk: false,
  };
}

function topTokenList(tokenCounts: Map<string, number>, limit = 8): string[] {
  return [...tokenCounts.entries()]
    .sort((left, right) => right[1] - left[1])
    .slice(0, limit)
    .map(([token]) => token);
}

function buildSuggestions(input: {
  topTokens: string[];
  categoryA: string;
  categoryB: string;
}): {
  suggestedIncludeA: string[];
  suggestedExcludeA: string[];
  suggestedIncludeB: string[];
  suggestedExcludeB: string[];
} {
  const ruleA = getRule(input.categoryA);
  const ruleB = getRule(input.categoryB);

  const includeA = normalizeTerms(ruleA.include_any);
  const excludeA = normalizeTerms(ruleA.exclude_any);
  const includeB = normalizeTerms(ruleB.include_any);
  const excludeB = normalizeTerms(ruleB.exclude_any);

  const descriptorA = getDescriptorTokens(input.categoryA);
  const descriptorB = getDescriptorTokens(input.categoryB);

  const suggestedIncludeA: string[] = [];
  const suggestedExcludeA: string[] = [];
  const suggestedIncludeB: string[] = [];
  const suggestedExcludeB: string[] = [];

  for (const token of input.topTokens) {
    if (
      descriptorA.has(token) &&
      !includeA.has(token) &&
      !includeB.has(token) &&
      suggestedIncludeA.length < 5
    ) {
      suggestedIncludeA.push(token);
    }

    if (includeB.has(token) && !excludeA.has(token) && suggestedExcludeA.length < 5) {
      suggestedExcludeA.push(token);
    }

    if (
      descriptorB.has(token) &&
      !includeB.has(token) &&
      !includeA.has(token) &&
      suggestedIncludeB.length < 5
    ) {
      suggestedIncludeB.push(token);
    }

    if (includeA.has(token) && !excludeB.has(token) && suggestedExcludeB.length < 5) {
      suggestedExcludeB.push(token);
    }
  }

  return {
    suggestedIncludeA,
    suggestedExcludeA,
    suggestedIncludeB,
    suggestedExcludeB,
  };
}

function extractTokens(text: string): string[] {
  return tokenize(text).filter((token) => token.length >= 3 && !/^\d+$/.test(token));
}

export function buildConfusionHotlist(input: {
  products: NormalizedCatalogProduct[];
  assignmentsBySku: Map<string, CategoryAssignment>;
  maxRows?: number;
}): ConfusionHotlistResult {
  const maxRows = Math.max(1, input.maxRows ?? 20);
  const productBySku = new Map<string, NormalizedCatalogProduct>();
  for (const product of input.products) {
    productBySku.set(product.sourceSku, product);
  }

  const buckets = new Map<string, ConfusionBucket>();

  for (const assignment of input.assignmentsBySku.values()) {
    if (!shouldIncludeForConfusion(assignment)) {
      continue;
    }

    const top2 = assignment.categoryTop2Slug;
    if (!top2) {
      continue;
    }

    const product = productBySku.get(assignment.sourceSku);
    if (!product) {
      continue;
    }

    const [categoryA, categoryB] = sortPair(assignment.categorySlug, top2);
    const bucketKey = `${categoryA}<->${categoryB}`;

    const existing = buckets.get(bucketKey) ?? {
      categoryA,
      categoryB,
      assignments: [],
      tokenCounts: new Map<string, number>(),
      lowMarginCount: 0,
      contradictionCount: 0,
    };

    existing.assignments.push({ assignment, product });

    if (assignment.categoryMargin < 0.12) {
      existing.lowMarginCount += 1;
    }
    if (assignment.categoryContradictionCount > 0) {
      existing.contradictionCount += assignment.categoryContradictionCount;
    }

    const text = normalizeText(`${product.title} ${product.description ?? ""}`);
    for (const token of extractTokens(text)) {
      existing.tokenCounts.set(token, (existing.tokenCounts.get(token) ?? 0) + 1);
    }

    buckets.set(bucketKey, existing);
  }

  const rows: ConfusionHotlistRow[] = [...buckets.values()]
    .map((bucket) => {
      const sortedAssignments = [...bucket.assignments].sort(
        (left, right) =>
          right.assignment.categoryContradictionCount + (left.assignment.categoryMargin < 0.12 ? 1 : 0) -
          (left.assignment.categoryContradictionCount + (right.assignment.categoryMargin < 0.12 ? 1 : 0)),
      );
      const sampleAssignments = sortedAssignments.slice(0, 5);

      const topTokens = topTokenList(bucket.tokenCounts, 10);
      const suggestions = buildSuggestions({
        topTokens,
        categoryA: bucket.categoryA,
        categoryB: bucket.categoryB,
      });

      return {
        category_a: bucket.categoryA,
        category_b: bucket.categoryB,
        affected_count: bucket.assignments.length,
        low_margin_count: bucket.lowMarginCount,
        contradiction_count: bucket.contradictionCount,
        sample_skus: sampleAssignments.map((entry) => entry.assignment.sourceSku).join(" | "),
        sample_titles: sampleAssignments.map((entry) => entry.product.title).join(" | "),
        top_tokens: topTokens.join(" | "),
        suggested_include_a: suggestions.suggestedIncludeA.join(" | "),
        suggested_exclude_a: suggestions.suggestedExcludeA.join(" | "),
        suggested_include_b: suggestions.suggestedIncludeB.join(" | "),
        suggested_exclude_b: suggestions.suggestedExcludeB.join(" | "),
      };
    })
    .sort((left, right) => {
      const leftScore = left.affected_count * 3 + left.low_margin_count * 2 + left.contradiction_count;
      const rightScore = right.affected_count * 3 + right.low_margin_count * 2 + right.contradiction_count;
      return rightScore - leftScore;
    })
    .slice(0, maxRows);

  return {
    rows,
    csvContent: toCsv(rows),
  };
}

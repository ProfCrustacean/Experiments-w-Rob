import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type {
  TaxonomyAttributePolicyConfig,
  TaxonomyCategory,
  TaxonomyCategoryMatchRule,
} from "../types.js";

interface CategoriesFile {
  schema_version: string;
  categories: TaxonomyCategory[];
}

interface MatchRulesFile {
  schema_version: string;
  categories: TaxonomyCategoryMatchRule[];
}

interface LegacySplitFile {
  schema_version: string;
  legacy_to_family: Record<string, string>;
}

export interface LoadedTaxonomy {
  categoriesVersion: string;
  matchRulesVersion: string;
  legacySplitVersion: string;
  taxonomyVersion: string;
  categories: TaxonomyCategory[];
  rulesBySlug: Map<string, TaxonomyCategoryMatchRule>;
  categoriesBySlug: Map<string, TaxonomyCategory>;
  legacyToFamily: Record<string, string>;
  legacyByFamily: Map<string, string[]>;
  fallbackCategory: TaxonomyCategory;
  attributePolicies: TaxonomyAttributePolicyConfig;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let cachedTaxonomy: LoadedTaxonomy | null = null;

function readJsonFile<T>(fileName: string): T {
  const filePath = path.join(__dirname, fileName);
  const content = readFileSync(filePath, "utf8");
  return JSON.parse(content) as T;
}

export function loadTaxonomy(): LoadedTaxonomy {
  if (cachedTaxonomy) {
    return cachedTaxonomy;
  }

  const categoriesFile = readJsonFile<CategoriesFile>("categories.pt.json");
  const matchRulesFile = readJsonFile<MatchRulesFile>("category_match_rules.pt.json");
  const attributePolicies = readJsonFile<TaxonomyAttributePolicyConfig>("attribute_policies.pt.json");
  const legacySplitFile = readJsonFile<LegacySplitFile>("legacy_split_to_family.pt.json");

  if (!Array.isArray(categoriesFile.categories) || categoriesFile.categories.length === 0) {
    throw new Error("Taxonomy categories file is empty.");
  }

  const categoriesBySlug = new Map<string, TaxonomyCategory>();
  for (const category of categoriesFile.categories) {
    if (!category.slug) {
      throw new Error("Taxonomy category missing slug.");
    }
    categoriesBySlug.set(category.slug, category);
  }

  const rulesBySlug = new Map<string, TaxonomyCategoryMatchRule>();
  for (const rule of matchRulesFile.categories) {
    if (!categoriesBySlug.has(rule.slug)) {
      throw new Error(`Taxonomy rule references unknown category slug: ${rule.slug}`);
    }
    rulesBySlug.set(rule.slug, rule);
  }

  const fallbackCategory =
    categoriesFile.categories.find((category) => category.is_fallback) ??
    categoriesFile.categories.find((category) => category.slug === "outros_escolares");

  if (!fallbackCategory) {
    throw new Error("Taxonomy is missing a fallback category.");
  }

  const legacyByFamily = new Map<string, string[]>();
  for (const [legacySlug, familySlug] of Object.entries(legacySplitFile.legacy_to_family ?? {})) {
    const list = legacyByFamily.get(familySlug) ?? [];
    list.push(legacySlug);
    legacyByFamily.set(familySlug, list);
  }

  const taxonomyVersion = `${categoriesFile.schema_version}|${matchRulesFile.schema_version}`;

  cachedTaxonomy = {
    categoriesVersion: categoriesFile.schema_version,
    matchRulesVersion: matchRulesFile.schema_version,
    legacySplitVersion: legacySplitFile.schema_version,
    taxonomyVersion,
    categories: categoriesFile.categories,
    rulesBySlug,
    categoriesBySlug,
    legacyToFamily: legacySplitFile.legacy_to_family ?? {},
    legacyByFamily,
    fallbackCategory,
    attributePolicies,
  };

  return cachedTaxonomy;
}

export function __test_only_resetTaxonomyCache(): void {
  cachedTaxonomy = null;
}

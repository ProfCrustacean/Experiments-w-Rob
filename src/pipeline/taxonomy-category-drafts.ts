import type { CategoryDraft, TaxonomyCategory } from "../types.js";
import { loadTaxonomy } from "../taxonomy/load.js";
import { buildExactSchemaShape } from "./category-generation.js";
import { uniqueStrings } from "../utils/collections.js";

export function buildCategoryDraftsFromTaxonomyAssignments(input: {
  assignedCategoryBySku: Map<string, string>;
}): CategoryDraft[] {
  const taxonomy = loadTaxonomy();
  const skuByCategorySlug = new Map<string, string[]>();

  for (const [sourceSku, categorySlug] of input.assignedCategoryBySku.entries()) {
    const existing = skuByCategorySlug.get(categorySlug) ?? [];
    existing.push(sourceSku);
    skuByCategorySlug.set(categorySlug, existing);
  }

  const drafts: CategoryDraft[] = [];
  for (const [categorySlug, skus] of skuByCategorySlug.entries()) {
    const category = taxonomy.categoriesBySlug.get(categorySlug);
    if (!category) {
      continue;
    }

    drafts.push(buildDraftFromTaxonomyCategory(category, skus));
  }

  return drafts;
}

function buildDraftFromTaxonomyCategory(category: TaxonomyCategory, sourceProductSkus: string[]): CategoryDraft {
  const synonyms = uniqueStrings([category.name_pt, ...category.synonyms, ...category.prototype_terms]);

  return {
    name_pt: category.name_pt,
    slug: category.slug,
    description_pt: category.description_pt,
    attributes_jsonb: buildExactSchemaShape(category.name_pt, category.default_attributes),
    synonyms,
    sourceProductSkus,
  };
}

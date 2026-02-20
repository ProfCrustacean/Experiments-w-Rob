import pLimit from "p-limit";
import type {
  CategoryAttribute,
  CategoryAttributeSchema,
  CategoryCluster,
  CategoryDraft,
  LLMProvider,
} from "../types.js";
import { CATEGORY_HINT_RULES, FALLBACK_CATEGORY_RULE } from "./category-rules.js";
import { makeSlug, normalizeText, shortSpecificName, titleCase } from "../utils/text.js";
import { uniqueStrings } from "../utils/collections.js";

function getRuleDefaults(clusterKey: string): CategoryAttribute[] {
  const prefix = clusterKey.split("-")[0];
  const matched = CATEGORY_HINT_RULES.find((rule) => rule.key === prefix);
  return matched?.defaultAttributes ?? FALLBACK_CATEGORY_RULE.defaultAttributes;
}

function sanitizeAttribute(attribute: CategoryAttribute): CategoryAttribute {
  const sanitizedKey = normalizeText(attribute.key).replace(/\s+/g, "_").replace(/[^a-z0-9_]/g, "");
  const safeType = ["enum", "number", "boolean", "text"].includes(attribute.type)
    ? attribute.type
    : "text";

  const base: CategoryAttribute = {
    key: sanitizedKey || "atributo",
    label_pt: titleCase(normalizeText(attribute.label_pt).replace(/_/g, " ")) || "Atributo",
    type: safeType,
    required: Boolean(attribute.required),
  };

  if (safeType === "enum") {
    base.allowed_values = uniqueStrings(attribute.allowed_values ?? []);
    if (!base.allowed_values || base.allowed_values.length === 0) {
      base.allowed_values = ["outro"];
    }
  }

  return base;
}

function buildSchema(categoryName: string, attributes: CategoryAttribute[]): CategoryAttributeSchema {
  const deduped = new Map<string, CategoryAttribute>();

  for (const attribute of attributes.map(sanitizeAttribute)) {
    if (!deduped.has(attribute.key)) {
      deduped.set(attribute.key, attribute);
    }
  }

  return {
    schema_version: "1.0",
    category_name_pt: normalizeText(categoryName),
    attributes: [...deduped.values()],
  };
}

function buildFallbackCategoryDraft(cluster: CategoryCluster): CategoryDraft {
  const name = shortSpecificName(cluster.candidateName);
  const attributes = getRuleDefaults(cluster.key);

  return {
    name_pt: name,
    slug: makeSlug(name),
    description_pt: [
      `${name} para listas escolares.`,
      "Inclui apenas produtos desta subcategoria e exclui itens não compatíveis.",
      "Usada para recomendações rápidas e consistentes no catálogo.",
    ].join(" "),
    attributes_jsonb: buildSchema(name, attributes),
    synonyms: [name, ...cluster.key.split("-")],
    sourceProductSkus: cluster.products.map((product) => product.sourceSku),
  };
}

export async function generateCategoryDrafts(
  clusters: CategoryCluster[],
  llm: LLMProvider | null,
  concurrency = 1,
  onProgress?: (done: number, total: number) => void,
): Promise<{
  drafts: CategoryDraft[];
  clusterKeyToSlug: Record<string, string>;
}> {
  const draftsByCluster: CategoryDraft[] = new Array(clusters.length);
  const safeConcurrency = Math.max(1, Math.floor(concurrency));
  const limiter = pLimit(safeConcurrency);
  let done = 0;

  await Promise.all(
    clusters.map((cluster, index) =>
      limiter(async () => {
        let draft: CategoryDraft | null = null;

        if (llm) {
          try {
            const response = await llm.generateCategoryProfile({
              candidateName: cluster.candidateName,
              sampleProducts: cluster.products.slice(0, 15).map((product) => ({
                title: product.title,
                description: product.description,
                brand: product.brand,
              })),
            });

            const name = shortSpecificName(response.name_pt || cluster.candidateName);
            const attributes =
              response.attributes.length > 0 ? response.attributes : getRuleDefaults(cluster.key);

            draft = {
              name_pt: name,
              slug: makeSlug(name),
              description_pt: response.description_pt,
              attributes_jsonb: buildSchema(name, attributes),
              synonyms: uniqueStrings([name, ...response.synonyms]),
              sourceProductSkus: cluster.products.map((product) => product.sourceSku),
            };
          } catch {
            draft = null;
          }
        }

        if (!draft) {
          draft = buildFallbackCategoryDraft(cluster);
        }

        draftsByCluster[index] = draft;
        done += 1;
        onProgress?.(done, clusters.length);
      }),
    ),
  );

  const drafts: CategoryDraft[] = [];
  const clusterKeyToSlug: Record<string, string> = {};
  const usedSlugs = new Set<string>();

  for (let index = 0; index < clusters.length; index += 1) {
    const cluster = clusters[index];
    const draft = draftsByCluster[index] ?? buildFallbackCategoryDraft(cluster);

    let finalSlug = draft.slug;
    let counter = 2;
    while (usedSlugs.has(finalSlug)) {
      finalSlug = `${draft.slug}-${counter}`;
      counter += 1;
    }

    usedSlugs.add(finalSlug);
    draft.slug = finalSlug;

    drafts.push(draft);
    clusterKeyToSlug[cluster.key] = draft.slug;
  }

  return { drafts, clusterKeyToSlug };
}

export function buildExactSchemaShape(
  categoryName: string,
  attributes: CategoryAttribute[],
): CategoryAttributeSchema {
  return buildSchema(categoryName, attributes);
}

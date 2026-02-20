import type { CategoryCluster, NormalizedCatalogProduct } from "../types.js";
import {
  CATEGORY_HINT_RULES,
  FALLBACK_CATEGORY_RULE,
  type CategoryHintRule,
} from "./category-rules.js";
import {
  detectFormat,
  detectPackCount,
  detectRuling,
  normalizeText,
  shortSpecificName,
  tokenize,
} from "../utils/text.js";

interface CategoryDecision {
  rule: CategoryHintRule;
  score: number;
  clusterSuffix: string;
}

function scoreRule(product: NormalizedCatalogProduct, rule: CategoryHintRule): number {
  let score = 0;

  for (const keyword of rule.keywords) {
    const normalizedKeyword = normalizeText(keyword);
    if (!normalizedKeyword) {
      continue;
    }

    if (product.normalizedText.includes(normalizedKeyword)) {
      score += 1;
      if (product.normalizedTitle.includes(normalizedKeyword)) {
        score += 1;
      }
    }
  }

  return score;
}

function deriveFallbackSuffix(text: string): string {
  const tokens = tokenize(text).slice(0, 2);
  if (tokens.length === 0) {
    return "geral";
  }
  return tokens.join("-");
}

function notebookSuffix(product: NormalizedCatalogProduct): string {
  const format = detectFormat(product.normalizedText) ?? "sem-formato";
  const ruling = detectRuling(product.normalizedText) ?? "geral";
  return `${format.toLowerCase()}-${ruling}`;
}

function penSuffix(product: NormalizedCatalogProduct): string {
  if (/gel/.test(product.normalizedText)) {
    return "gel";
  }
  if (/(esferografica|esferografico)/.test(product.normalizedText)) {
    return "esferografica";
  }
  if (/roller/.test(product.normalizedText)) {
    return "roller";
  }
  return "geral";
}

function pencilSuffix(product: NormalizedCatalogProduct): string {
  const hardness = product.normalizedText.match(/\b(hb|2b|b|h)\b/i)?.[1]?.toUpperCase();
  if (hardness) {
    return hardness.toLowerCase();
  }
  if (/cor/.test(product.normalizedText)) {
    return "cor";
  }
  return "grafite";
}

function deriveCategoryDecision(product: NormalizedCatalogProduct): CategoryDecision {
  let bestRule = FALLBACK_CATEGORY_RULE;
  let bestScore = -1;

  for (const rule of CATEGORY_HINT_RULES) {
    const score = scoreRule(product, rule);
    if (score > bestScore) {
      bestRule = rule;
      bestScore = score;
    }
  }

  if (bestScore <= 0) {
    return {
      rule: FALLBACK_CATEGORY_RULE,
      score: 0.35,
      clusterSuffix: deriveFallbackSuffix(product.normalizedText),
    };
  }

  let suffix = "geral";
  if (bestRule.key === "caderno") {
    suffix = notebookSuffix(product);
  } else if (bestRule.key === "caneta") {
    suffix = penSuffix(product);
  } else if (bestRule.key === "lapis") {
    suffix = pencilSuffix(product);
  } else {
    const packCount = detectPackCount(product.normalizedText);
    if (packCount) {
      suffix = `${packCount}un`;
    }
  }

  const confidence = Math.min(0.95, 0.5 + bestScore * 0.08);
  return {
    rule: bestRule,
    score: confidence,
    clusterSuffix: suffix,
  };
}

export function clusterProducts(
  products: NormalizedCatalogProduct[],
): {
  clusters: CategoryCluster[];
  skuToClusterKey: Record<string, string>;
} {
  const map = new Map<string, CategoryCluster>();
  const skuToClusterKey: Record<string, string> = {};

  for (const product of products) {
    const decision = deriveCategoryDecision(product);
    const clusterKey = `${decision.rule.key}-${decision.clusterSuffix}`;

    let cluster = map.get(clusterKey);
    if (!cluster) {
      cluster = {
        key: clusterKey,
        candidateName: shortSpecificName(`${decision.rule.labelPt} ${decision.clusterSuffix}`),
        products: [],
        scoresBySku: {},
      };
      map.set(clusterKey, cluster);
    }

    cluster.products.push(product);
    cluster.scoresBySku[product.sourceSku] = decision.score;
    skuToClusterKey[product.sourceSku] = clusterKey;
  }

  return { clusters: [...map.values()], skuToClusterKey };
}

import { readFile, writeFile } from "node:fs/promises";
import { __test_only_resetTaxonomyCache } from "../taxonomy/load.js";
import type { SelfImproveProposalPayload } from "../types.js";

export interface CategoryRuleFile {
  schema_version: string;
  categories: Array<{
    slug: string;
    include_any: string[];
    include_all: string[];
    exclude_any: string[];
    strong_exclude_any: string[];
    high_risk: boolean;
    out_of_scope?: boolean;
    auto_min_confidence?: number;
    auto_min_margin?: number;
  }>;
}

function safeJsonClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function normalizeTerm(value: string): string {
  return value.trim().toLowerCase();
}

export function dedupeTerms(values: string[]): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed) {
      continue;
    }
    const key = normalizeTerm(trimmed);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    output.push(trimmed);
  }
  return output;
}

export async function readCategoryRulesFile(filePath: string): Promise<CategoryRuleFile> {
  const content = await readFile(filePath, "utf8");
  const parsed = JSON.parse(content) as CategoryRuleFile;
  if (!Array.isArray(parsed.categories)) {
    throw new Error("Invalid category_match_rules.pt.json: categories array is missing.");
  }
  return parsed;
}

export async function writeCategoryRulesFile(filePath: string, data: CategoryRuleFile): Promise<void> {
  await writeFile(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
  __test_only_resetTaxonomyCache();
}

export function applyProposalPayloadToRules(input: {
  rules: CategoryRuleFile;
  payload: SelfImproveProposalPayload;
  fallbackOldValue?: string | number | string[];
}): {
  updatedRules: CategoryRuleFile;
  oldValue: string | number | string[] | null;
  newValue: string | number | string[] | null;
} {
  const updatedRules = safeJsonClone(input.rules);
  const targetRule = updatedRules.categories.find((rule) => rule.slug === input.payload.target_slug);
  if (!targetRule) {
    throw new Error(`Cannot apply proposal: unknown rule slug '${input.payload.target_slug}'.`);
  }

  const field = input.payload.field;
  const action = input.payload.action;
  const rawValue = input.payload.value;

  if (
    field === "include_any" ||
    field === "exclude_any" ||
    field === "strong_exclude_any"
  ) {
    if (typeof rawValue !== "string") {
      throw new Error(`Expected string value for ${field}.`);
    }
    const current = Array.isArray(targetRule[field]) ? [...targetRule[field]] : [];
    const oldValue = [...current];

    if (action === "add") {
      current.push(rawValue);
      targetRule[field] = dedupeTerms(current);
    } else if (action === "remove") {
      const removeKey = normalizeTerm(rawValue);
      targetRule[field] = current.filter((entry) => normalizeTerm(entry) !== removeKey);
    } else if (action === "set") {
      targetRule[field] = dedupeTerms([rawValue]);
    } else {
      throw new Error(`Unsupported action '${action}' for field '${field}'.`);
    }

    return {
      updatedRules,
      oldValue,
      newValue: [...targetRule[field]],
    };
  }

  if (field === "auto_min_confidence" || field === "auto_min_margin") {
    const nextValue = Number(rawValue);
    if (!Number.isFinite(nextValue)) {
      throw new Error(`Expected numeric value for ${field}.`);
    }

    const oldValue =
      input.fallbackOldValue !== undefined
        ? (input.fallbackOldValue as number)
        : ((targetRule[field] ?? null) as number | null);

    if (action !== "set") {
      throw new Error(`Only 'set' action is allowed for numeric field '${field}'.`);
    }

    targetRule[field] = nextValue;
    return {
      updatedRules,
      oldValue,
      newValue: nextValue,
    };
  }

  throw new Error(`Unsupported proposal field '${field}'.`);
}

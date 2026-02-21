import { mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { parse } from "csv-parse/sync";
import { deduplicateRows, readCatalogFile } from "../pipeline/ingest.js";
import type { RawCatalogRow } from "../types.js";

const HOTLIST_FILE_PATTERN = /^confusion_hotlist_.*\.csv$/;

interface HotlistCsvRow {
  category_a?: string;
  category_b?: string;
  affected_count?: string;
  low_margin_count?: string;
  contradiction_count?: string;
  sample_skus?: string;
}

interface ParsedHotlistRow {
  categoryA: string;
  categoryB: string;
  affectedCount: number;
  lowMarginCount: number;
  contradictionCount: number;
  sampleSkus: string;
}

export interface CanaryState {
  lastCanaryRunId: string;
  lastCanaryHotlistPath: string;
  updatedAt: string;
}

export type CanaryHotlistSourceKind = "state" | "latest_local" | "none";

export interface CanaryHotlistSource {
  kind: CanaryHotlistSourceKind;
  path: string | null;
}

export interface BuildCanarySubsetInput {
  inputPath: string;
  outputDir: string;
  subsetPath: string;
  statePath: string;
  sampleSize: number;
  fixedRatio: number;
  randomSeed: string;
  storeId: string;
}

export interface BuildCanarySubsetResult {
  subsetPath: string;
  sampleSizeRequested: number;
  sampleSizeUsed: number;
  totalAvailable: number;
  fixedTarget: number;
  fixedSelected: number;
  randomSelected: number;
  hotlistSource: CanaryHotlistSourceKind;
  hotlistPath: string | null;
  selectedSkus: string[];
  warnings: string[];
}

function toNumber(value: string | undefined): number {
  if (!value) {
    return 0;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function hashString(value: string): number {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return Math.abs(hash >>> 0);
}

function escapeCsv(value: string): string {
  if (/[",\n]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function toCsvContent(rows: RawCatalogRow[]): string {
  const header = [
    "source_sku",
    "title",
    "description",
    "brand",
    "price",
    "availability",
    "url",
    "image_url",
  ];

  const body = rows.map((row) => {
    const line = [
      row.sourceSku,
      row.title,
      row.description ?? "",
      row.brand ?? "",
      row.price === undefined ? "" : String(row.price),
      row.availability ?? "",
      row.url ?? "",
      row.imageUrl ?? "",
    ];

    return line.map(escapeCsv).join(",");
  });

  return [header.join(","), ...body].join("\n");
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    const details = await stat(filePath);
    return details.isFile();
  } catch {
    return false;
  }
}

export async function readCanaryState(statePath: string): Promise<CanaryState | null> {
  try {
    const content = await readFile(statePath, "utf8");
    const parsed = JSON.parse(content) as Partial<CanaryState>;

    if (
      typeof parsed.lastCanaryRunId !== "string" ||
      parsed.lastCanaryRunId.trim().length === 0 ||
      typeof parsed.lastCanaryHotlistPath !== "string" ||
      parsed.lastCanaryHotlistPath.trim().length === 0 ||
      typeof parsed.updatedAt !== "string" ||
      parsed.updatedAt.trim().length === 0
    ) {
      return null;
    }

    return {
      lastCanaryRunId: parsed.lastCanaryRunId,
      lastCanaryHotlistPath: parsed.lastCanaryHotlistPath,
      updatedAt: parsed.updatedAt,
    };
  } catch {
    return null;
  }
}

export async function writeCanaryState(input: {
  statePath: string;
  runId: string;
  hotlistPath: string;
  now?: Date;
}): Promise<CanaryState> {
  const state: CanaryState = {
    lastCanaryRunId: input.runId,
    lastCanaryHotlistPath: input.hotlistPath,
    updatedAt: (input.now ?? new Date()).toISOString(),
  };

  await mkdir(path.dirname(input.statePath), { recursive: true });
  await writeFile(input.statePath, JSON.stringify(state, null, 2), "utf8");
  return state;
}

export async function resolveCanaryHotlistSource(input: {
  statePath: string;
  outputDir: string;
}): Promise<CanaryHotlistSource> {
  const state = await readCanaryState(input.statePath);
  if (state) {
    const stateHotlistPath = path.isAbsolute(state.lastCanaryHotlistPath)
      ? state.lastCanaryHotlistPath
      : path.resolve(path.dirname(input.statePath), state.lastCanaryHotlistPath);

    if (await fileExists(stateHotlistPath)) {
      return {
        kind: "state",
        path: stateHotlistPath,
      };
    }
  }

  const entries = await readdir(input.outputDir, {
    withFileTypes: true,
    encoding: "utf8",
  }).catch(() => null);

  if (!entries) {
    return {
      kind: "none",
      path: null,
    };
  }

  const candidates: Array<{ path: string; mtimeMs: number }> = [];

  for (const entry of entries) {
    if (!entry.isFile()) {
      continue;
    }
    if (!HOTLIST_FILE_PATTERN.test(entry.name)) {
      continue;
    }

    const candidatePath = path.join(input.outputDir, entry.name);
    const details = await stat(candidatePath);
    candidates.push({
      path: candidatePath,
      mtimeMs: details.mtimeMs,
    });
  }

  if (candidates.length === 0) {
    return {
      kind: "none",
      path: null,
    };
  }

  candidates.sort((left, right) => {
    if (right.mtimeMs !== left.mtimeMs) {
      return right.mtimeMs - left.mtimeMs;
    }
    return right.path.localeCompare(left.path);
  });

  return {
    kind: "latest_local",
    path: candidates[0].path,
  };
}

function parseHotlistRows(content: string): ParsedHotlistRow[] {
  const rows = parse(content, {
    columns: true,
    skip_empty_lines: true,
    bom: true,
    trim: true,
  }) as HotlistCsvRow[];

  return rows.map((row) => ({
    categoryA: row.category_a ?? "",
    categoryB: row.category_b ?? "",
    affectedCount: toNumber(row.affected_count),
    lowMarginCount: toNumber(row.low_margin_count),
    contradictionCount: toNumber(row.contradiction_count),
    sampleSkus: row.sample_skus ?? "",
  }));
}

function sortHotlistRows(rows: ParsedHotlistRow[]): ParsedHotlistRow[] {
  return [...rows].sort((left, right) => {
    if (right.affectedCount !== left.affectedCount) {
      return right.affectedCount - left.affectedCount;
    }
    if (right.lowMarginCount !== left.lowMarginCount) {
      return right.lowMarginCount - left.lowMarginCount;
    }
    if (right.contradictionCount !== left.contradictionCount) {
      return right.contradictionCount - left.contradictionCount;
    }
    if (left.categoryA !== right.categoryA) {
      return left.categoryA.localeCompare(right.categoryA);
    }
    return left.categoryB.localeCompare(right.categoryB);
  });
}

function splitSampleSkus(sampleSkus: string): string[] {
  return sampleSkus
    .split("|")
    .map((sku) => sku.trim())
    .filter(Boolean);
}

function deterministicSkuOrder(input: {
  skus: string[];
  randomSeed: string;
  storeId: string;
}): string[] {
  return [...input.skus].sort((left, right) => {
    const leftRank = hashString(`${input.randomSeed}::${input.storeId}::${left}`);
    const rightRank = hashString(`${input.randomSeed}::${input.storeId}::${right}`);
    if (leftRank !== rightRank) {
      return leftRank - rightRank;
    }
    return left.localeCompare(right);
  });
}

export async function buildCanarySubset(input: BuildCanarySubsetInput): Promise<BuildCanarySubsetResult> {
  const warnings: string[] = [];
  const rawRows = await readCatalogFile(input.inputPath);
  const deduplicated = deduplicateRows(rawRows);

  if (deduplicated.length === 0) {
    throw new Error("Canary input has no valid products after ingest/deduplication.");
  }

  const sampleSizeUsed = Math.min(Math.max(1, input.sampleSize), deduplicated.length);
  if (sampleSizeUsed < input.sampleSize) {
    warnings.push(
      `Only ${deduplicated.length} unique products available. Canary sample reduced from ${input.sampleSize} to ${sampleSizeUsed}.`,
    );
  }

  const fixedTarget = Math.min(
    sampleSizeUsed,
    Math.max(1, Math.round(sampleSizeUsed * input.fixedRatio)),
  );

  const source = await resolveCanaryHotlistSource({
    statePath: input.statePath,
    outputDir: input.outputDir,
  });

  if (source.kind === "none") {
    warnings.push("No hotlist source was found. Fixed canary products will be backfilled using random selection.");
  }

  const validSkuSet = new Set(deduplicated.map((row) => row.sourceSku));
  const fixedSkus: string[] = [];
  const fixedSet = new Set<string>();

  if (source.path) {
    const hotlistContent = await readFile(source.path, "utf8");
    const sortedHotlist = sortHotlistRows(parseHotlistRows(hotlistContent));

    for (const row of sortedHotlist) {
      for (const sku of splitSampleSkus(row.sampleSkus)) {
        if (!validSkuSet.has(sku) || fixedSet.has(sku)) {
          continue;
        }

        fixedSet.add(sku);
        fixedSkus.push(sku);

        if (fixedSkus.length >= fixedTarget) {
          break;
        }
      }

      if (fixedSkus.length >= fixedTarget) {
        break;
      }
    }
  }

  if (fixedSkus.length < fixedTarget) {
    warnings.push(
      `Hotlist provided ${fixedSkus.length} fixed products for target ${fixedTarget}. Remaining slots were backfilled with random selection.`,
    );
  }

  const randomCandidates = deterministicSkuOrder({
    skus: deduplicated.map((row) => row.sourceSku).filter((sku) => !fixedSet.has(sku)),
    randomSeed: input.randomSeed,
    storeId: input.storeId,
  });

  const randomNeeded = sampleSizeUsed - fixedSkus.length;
  const randomSkus = randomCandidates.slice(0, randomNeeded);

  const selectedSkuSet = new Set([...fixedSkus, ...randomSkus]);
  const selectedRows = deduplicated.filter((row) => selectedSkuSet.has(row.sourceSku));

  await mkdir(path.dirname(input.subsetPath), { recursive: true });
  await writeFile(input.subsetPath, toCsvContent(selectedRows), "utf8");

  return {
    subsetPath: input.subsetPath,
    sampleSizeRequested: input.sampleSize,
    sampleSizeUsed,
    totalAvailable: deduplicated.length,
    fixedTarget,
    fixedSelected: fixedSkus.length,
    randomSelected: selectedRows.length - fixedSkus.length,
    hotlistSource: source.kind,
    hotlistPath: source.path,
    selectedSkus: selectedRows.map((row) => row.sourceSku),
    warnings,
  };
}

export function __test_only_splitSampleSkus(sampleSkus: string): string[] {
  return splitSampleSkus(sampleSkus);
}

export function __test_only_sortHotlistRows(rows: Array<{
  categoryA: string;
  categoryB: string;
  affectedCount: number;
  lowMarginCount: number;
  contradictionCount: number;
  sampleSkus: string;
}>): Array<{
  categoryA: string;
  categoryB: string;
  affectedCount: number;
  lowMarginCount: number;
  contradictionCount: number;
  sampleSkus: string;
}> {
  return sortHotlistRows(rows);
}

export function __test_only_deterministicSkuOrder(input: {
  skus: string[];
  randomSeed: string;
  storeId: string;
}): string[] {
  return deterministicSkuOrder(input);
}

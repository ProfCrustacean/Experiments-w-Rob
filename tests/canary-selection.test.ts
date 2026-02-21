import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { buildCanarySubset } from "../src/canary/select-subset.js";

function buildCatalogCsv(count: number): string {
  const lines = ["source_sku,title,description,brand"];
  for (let index = 1; index <= count; index += 1) {
    lines.push(`sku-${index},Produto ${index},Descricao ${index},Marca ${index}`);
  }
  return lines.join("\n");
}

function buildHotlistCsv(skus: string[]): string {
  return [
    "category_a,category_b,affected_count,low_margin_count,contradiction_count,sample_skus,sample_titles,top_tokens,suggested_include_a,suggested_exclude_a,suggested_include_b,suggested_exclude_b",
    `escrita,outros_escolares,999,200,10,${skus.join(" | ")},,,,,,`,
  ].join("\n");
}

describe("canary subset selection", () => {
  it("builds 105 fixed + 245 random products for a 350-product canary", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "canary-selection-"));
    const outputDir = path.join(dir, "outputs");
    await mkdir(outputDir, { recursive: true });

    const catalogPath = path.join(dir, "catalog.csv");
    const hotlistPath = path.join(outputDir, "confusion_hotlist_seed.csv");
    const subsetPath = path.join(outputDir, "canary_input.csv");
    const statePath = path.join(outputDir, "canary_state.json");

    await writeFile(catalogPath, buildCatalogCsv(500), "utf8");
    await writeFile(
      hotlistPath,
      buildHotlistCsv(Array.from({ length: 200 }, (_, index) => `sku-${index + 1}`)),
      "utf8",
    );

    const first = await buildCanarySubset({
      inputPath: catalogPath,
      outputDir,
      subsetPath,
      statePath,
      sampleSize: 350,
      fixedRatio: 0.3,
      randomSeed: "canary-v1",
      storeId: "continente",
    });

    const second = await buildCanarySubset({
      inputPath: catalogPath,
      outputDir,
      subsetPath,
      statePath,
      sampleSize: 350,
      fixedRatio: 0.3,
      randomSeed: "canary-v1",
      storeId: "continente",
    });

    expect(first.sampleSizeUsed).toBe(350);
    expect(first.fixedTarget).toBe(105);
    expect(first.fixedSelected).toBe(105);
    expect(first.randomSelected).toBe(245);
    expect(first.hotlistSource).toBe("latest_local");
    expect(first.selectedSkus).toEqual(second.selectedSkus);
  });

  it("backfills fixed slots with deterministic random selection when hotlist SKUs are missing", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "canary-backfill-"));
    const outputDir = path.join(dir, "outputs");
    await mkdir(outputDir, { recursive: true });

    const catalogPath = path.join(dir, "catalog.csv");
    const hotlistPath = path.join(outputDir, "confusion_hotlist_missing.csv");
    const subsetPath = path.join(outputDir, "canary_input.csv");
    const statePath = path.join(outputDir, "canary_state.json");

    await writeFile(catalogPath, buildCatalogCsv(400), "utf8");
    await writeFile(
      hotlistPath,
      buildHotlistCsv(Array.from({ length: 200 }, (_, index) => `missing-${index + 1}`)),
      "utf8",
    );

    const result = await buildCanarySubset({
      inputPath: catalogPath,
      outputDir,
      subsetPath,
      statePath,
      sampleSize: 350,
      fixedRatio: 0.3,
      randomSeed: "canary-v1",
      storeId: "continente",
    });

    expect(result.fixedTarget).toBe(105);
    expect(result.fixedSelected).toBe(0);
    expect(result.randomSelected).toBe(350);
    expect(result.warnings.some((warning) => warning.includes("backfilled"))).toBe(true);
  });

  it("falls back to random-only selection when no hotlist is available", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "canary-no-hotlist-"));
    const outputDir = path.join(dir, "outputs");
    await mkdir(outputDir, { recursive: true });

    const catalogPath = path.join(dir, "catalog.csv");
    const subsetPath = path.join(outputDir, "canary_input.csv");
    const statePath = path.join(outputDir, "canary_state.json");

    await writeFile(catalogPath, buildCatalogCsv(375), "utf8");

    const result = await buildCanarySubset({
      inputPath: catalogPath,
      outputDir,
      subsetPath,
      statePath,
      sampleSize: 350,
      fixedRatio: 0.3,
      randomSeed: "canary-v1",
      storeId: "continente",
    });

    expect(result.hotlistSource).toBe("none");
    expect(result.fixedSelected).toBe(0);
    expect(result.randomSelected).toBe(350);
    expect(result.warnings.some((warning) => warning.includes("No hotlist source"))).toBe(true);
  });
});

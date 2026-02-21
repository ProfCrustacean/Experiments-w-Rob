import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  readCanaryState,
  resolveCanaryHotlistSource,
  writeCanaryState,
} from "../src/canary/select-subset.js";

function hotlistCsv(id: string): string {
  return [
    "category_a,category_b,affected_count,low_margin_count,contradiction_count,sample_skus,sample_titles,top_tokens,suggested_include_a,suggested_exclude_a,suggested_include_b,suggested_exclude_b",
    `escrita,outros_escolares,10,5,2,sku-${id},,,,,,`,
  ].join("\n");
}

describe("canary state", () => {
  it("writes canary state and prefers state hotlist over generic latest hotlist", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "canary-state-"));
    const outputDir = path.join(dir, "outputs");
    await mkdir(outputDir, { recursive: true });

    const statePath = path.join(outputDir, "canary_state.json");
    const stateHotlistPath = path.join(outputDir, "confusion_hotlist_state.csv");
    const fallbackHotlistPath = path.join(outputDir, "confusion_hotlist_fallback.csv");

    await writeFile(stateHotlistPath, hotlistCsv("state"), "utf8");
    await writeFile(fallbackHotlistPath, hotlistCsv("fallback"), "utf8");

    await writeCanaryState({
      statePath,
      runId: "run-123",
      hotlistPath: stateHotlistPath,
      now: new Date("2026-02-21T16:00:00.000Z"),
    });

    const state = await readCanaryState(statePath);
    expect(state?.lastCanaryRunId).toBe("run-123");
    expect(state?.lastCanaryHotlistPath).toBe(stateHotlistPath);

    const source = await resolveCanaryHotlistSource({
      statePath,
      outputDir,
    });

    expect(source.kind).toBe("state");
    expect(source.path).toBe(stateHotlistPath);
  });

  it("falls back to latest local hotlist when state is invalid", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "canary-state-fallback-"));
    const outputDir = path.join(dir, "outputs");
    await mkdir(outputDir, { recursive: true });

    const statePath = path.join(outputDir, "canary_state.json");
    const hotlistPath = path.join(outputDir, "confusion_hotlist_latest.csv");

    await writeFile(statePath, "{broken_json", "utf8");
    await writeFile(hotlistPath, hotlistCsv("latest"), "utf8");

    const source = await resolveCanaryHotlistSource({
      statePath,
      outputDir,
    });

    expect(source.kind).toBe("latest_local");
    expect(source.path).toBe(hotlistPath);
  });

  it("returns none when no state and no local hotlist exist", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "canary-state-none-"));
    const outputDir = path.join(dir, "outputs");
    await mkdir(outputDir, { recursive: true });

    const source = await resolveCanaryHotlistSource({
      statePath: path.join(outputDir, "canary_state.json"),
      outputDir,
    });

    expect(source.kind).toBe("none");
    expect(source.path).toBeNull();
  });
});

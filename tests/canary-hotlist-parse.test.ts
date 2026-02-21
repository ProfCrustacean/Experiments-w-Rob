import { describe, expect, it } from "vitest";
import {
  __test_only_sortHotlistRows,
  __test_only_splitSampleSkus,
} from "../src/canary/select-subset.js";

describe("canary hotlist parsing", () => {
  it("splits sample_skus values robustly", () => {
    expect(__test_only_splitSampleSkus("sku-1 | sku-2|  sku-3  | ")).toEqual([
      "sku-1",
      "sku-2",
      "sku-3",
    ]);
  });

  it("sorts hotlist rows by severity and deterministic tie-breakers", () => {
    const sorted = __test_only_sortHotlistRows([
      {
        categoryA: "b",
        categoryB: "z",
        affectedCount: 10,
        lowMarginCount: 2,
        contradictionCount: 1,
        sampleSkus: "sku-2",
      },
      {
        categoryA: "a",
        categoryB: "z",
        affectedCount: 10,
        lowMarginCount: 2,
        contradictionCount: 1,
        sampleSkus: "sku-1",
      },
      {
        categoryA: "c",
        categoryB: "z",
        affectedCount: 20,
        lowMarginCount: 1,
        contradictionCount: 0,
        sampleSkus: "sku-3",
      },
      {
        categoryA: "d",
        categoryB: "z",
        affectedCount: 10,
        lowMarginCount: 5,
        contradictionCount: 1,
        sampleSkus: "sku-4",
      },
    ]);

    expect(sorted.map((row) => row.sampleSkus)).toEqual(["sku-3", "sku-4", "sku-1", "sku-2"]);
  });
});

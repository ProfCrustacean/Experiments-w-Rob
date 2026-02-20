import { describe, expect, it } from "vitest";
import {
  __test_only_normalizeRowsForPipeline,
  __test_only_partitionProductsBySample,
} from "../src/pipeline/run.js";

describe("run sampling", () => {
  it("selects a deterministic partition for the same store and settings", () => {
    const rows = __test_only_normalizeRowsForPipeline([
      { sourceSku: "sku-1", title: "Produto 1" },
      { sourceSku: "sku-2", title: "Produto 2" },
      { sourceSku: "sku-3", title: "Produto 3" },
      { sourceSku: "sku-4", title: "Produto 4" },
      { sourceSku: "sku-5", title: "Produto 5" },
      { sourceSku: "sku-6", title: "Produto 6" },
    ]);

    const first = __test_only_partitionProductsBySample(rows, "continente", 2, 0);
    const second = __test_only_partitionProductsBySample(rows, "continente", 2, 0);

    expect(first.sampled.map((row) => row.sourceSku)).toEqual(
      second.sampled.map((row) => row.sourceSku),
    );
    expect(first.skipped).toBe(second.skipped);
  });

  it("creates complementary halves for part 0 and part 1", () => {
    const rows = __test_only_normalizeRowsForPipeline([
      { sourceSku: "sku-1", title: "Produto 1" },
      { sourceSku: "sku-2", title: "Produto 2" },
      { sourceSku: "sku-3", title: "Produto 3" },
      { sourceSku: "sku-4", title: "Produto 4" },
      { sourceSku: "sku-5", title: "Produto 5" },
      { sourceSku: "sku-6", title: "Produto 6" },
      { sourceSku: "sku-7", title: "Produto 7" },
      { sourceSku: "sku-8", title: "Produto 8" },
    ]);

    const firstHalf = __test_only_partitionProductsBySample(rows, "continente", 2, 0);
    const secondHalf = __test_only_partitionProductsBySample(rows, "continente", 2, 1);

    const firstSkus = new Set(firstHalf.sampled.map((row) => row.sourceSku));
    const secondSkus = new Set(secondHalf.sampled.map((row) => row.sourceSku));

    for (const sku of firstSkus) {
      expect(secondSkus.has(sku)).toBe(false);
    }

    expect(firstSkus.size + secondSkus.size).toBe(rows.length);
  });
});

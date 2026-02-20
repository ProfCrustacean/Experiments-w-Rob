import { describe, expect, it } from "vitest";
import { FallbackProvider } from "../src/services/fallback.js";
import { generateEmbeddingsForItems } from "../src/pipeline/embedding.js";
import { __test_only_ensureVectorLength } from "../src/pipeline/run.js";

describe("embedding", () => {
  it("creates one vector per product with expected dimension", async () => {
    const provider = new FallbackProvider(3072);
    const items = [
      { sourceSku: "sku-1", text: "titulo: caderno a4 pautado" },
      { sourceSku: "sku-2", text: "titulo: caneta gel azul" },
    ];

    const vectorsBySku = await generateEmbeddingsForItems(items, provider, 2, 1);
    expect(vectorsBySku.size).toBe(2);
    expect(vectorsBySku.get("sku-1")?.length).toBe(3072);
  });

  it("pads vectors to the target size", () => {
    const output = __test_only_ensureVectorLength([1, 2, 3], 5);
    expect(output).toEqual([1, 2, 3, 0, 0]);
  });
});

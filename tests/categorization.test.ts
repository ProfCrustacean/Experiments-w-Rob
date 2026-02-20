import { describe, expect, it } from "vitest";
import { clusterProducts } from "../src/pipeline/categorization.js";

const baseProduct = {
  description: "",
  brand: "",
  price: undefined,
  availability: undefined,
  url: undefined,
  imageUrl: undefined,
};

describe("categorization", () => {
  it("clusters similar notebooks together and separates unrelated items", () => {
    const products = [
      {
        ...baseProduct,
        sourceSku: "a",
        title: "Caderno A4 pautado",
        normalizedTitle: "caderno a4 pautado",
        normalizedDescription: "96 folhas",
        normalizedBrand: "",
        normalizedText: "caderno a4 pautado 96 folhas",
      },
      {
        ...baseProduct,
        sourceSku: "b",
        title: "Notebook A4 ruled",
        normalizedTitle: "notebook a4 ruled",
        normalizedDescription: "",
        normalizedBrand: "",
        normalizedText: "notebook a4 ruled",
      },
      {
        ...baseProduct,
        sourceSku: "c",
        title: "Caneta gel azul",
        normalizedTitle: "caneta gel azul",
        normalizedDescription: "pack 12",
        normalizedBrand: "",
        normalizedText: "caneta gel azul pack 12",
      },
    ];

    const { clusters, skuToClusterKey } = clusterProducts(products);

    expect(clusters.length).toBeGreaterThanOrEqual(2);
    expect(skuToClusterKey.a).toEqual(skuToClusterKey.b);
    expect(skuToClusterKey.c).not.toEqual(skuToClusterKey.a);
  });
});

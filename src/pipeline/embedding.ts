import pLimit from "p-limit";
import type { EmbeddingProvider, NormalizedCatalogProduct } from "../types.js";
import { chunk } from "../utils/collections.js";

export interface EmbeddingWorkItem {
  sourceSku: string;
  text: string;
}

export function buildEmbeddingText(input: {
  product: NormalizedCatalogProduct;
  categoryName: string;
  attributeValues: Record<string, string | number | boolean | null>;
}): string {
  const attributes = Object.entries(input.attributeValues)
    .filter(([, value]) => value !== null && value !== "")
    .map(([key, value]) => `${key}: ${String(value)}`)
    .join("; ");

  return [
    `titulo: ${input.product.title}`,
    `marca: ${input.product.brand ?? ""}`,
    `descricao: ${input.product.description ?? ""}`,
    `categoria: ${input.categoryName}`,
    `atributos: ${attributes}`,
  ]
    .filter(Boolean)
    .join(" | ")
    .trim();
}

export async function generateEmbeddingsForItems(
  items: EmbeddingWorkItem[],
  provider: EmbeddingProvider,
  batchSize: number,
  concurrency: number,
): Promise<Map<string, number[]>> {
  const output = new Map<string, number[]>();
  const limiter = pLimit(concurrency);
  const groups = chunk(items, batchSize);

  await Promise.all(
    groups.map((group) =>
      limiter(async () => {
        const vectors = await provider.embedMany(group.map((item) => item.text));
        group.forEach((item, index) => {
          output.set(item.sourceSku, vectors[index]);
        });
      }),
    ),
  );

  return output;
}

export function vectorToSqlLiteral(vector: number[]): string {
  return `[${vector.join(",")}]`;
}

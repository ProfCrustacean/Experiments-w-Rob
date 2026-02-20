export function chunk<T>(items: T[], size: number): T[][] {
  if (size <= 0) {
    throw new Error("Chunk size must be greater than zero.");
  }

  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

export function sampleWithoutReplacement<T>(items: T[], sampleSize: number): T[] {
  if (sampleSize >= items.length) {
    return [...items];
  }

  const mutable = [...items];
  for (let i = mutable.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [mutable[i], mutable[j]] = [mutable[j], mutable[i]];
  }

  return mutable.slice(0, sampleSize);
}

export function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

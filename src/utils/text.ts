import slugifyModule from "slugify";

const STOP_WORDS = new Set([
  "de",
  "da",
  "do",
  "dos",
  "das",
  "e",
  "a",
  "o",
  "as",
  "os",
  "para",
  "com",
  "sem",
  "um",
  "uma",
  "na",
  "no",
  "em",
  "por",
  "kit",
  "escolar",
]);

export function normalizeText(value: string | undefined): string {
  if (!value) {
    return "";
  }

  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\p{L}\p{N}\s./-]/gu, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

export function tokenize(text: string): string[] {
  return normalizeText(text)
    .split(/\s+/)
    .map((token) => token.trim())
    .filter((token) => token.length > 1 && !STOP_WORDS.has(token));
}

export function makeSlug(input: string): string {
  const slugify = slugifyModule as unknown as (
    value: string,
    options?: {
      lower?: boolean;
      strict?: boolean;
      trim?: boolean;
    },
  ) => string;

  const slug = slugify(input, {
    lower: true,
    strict: true,
    trim: true,
  });

  return slug.length > 0 ? slug.slice(0, 64) : "categoria-sem-nome";
}

export function shortSpecificName(name: string): string {
  const cleaned = normalizeText(name)
    .replace(/\bcategoria\b/g, "")
    .replace(/\bprodutos?\b/g, "")
    .replace(/\bitens?\b/g, "")
    .replace(/\s+/g, " ")
    .trim();

  const capped = cleaned.slice(0, 42).trim();
  return capped.length > 0 ? titleCase(capped) : "Material Escolar";
}

export function titleCase(text: string): string {
  return text
    .split(" ")
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

export function detectFormat(text: string): string | null {
  const normalized = normalizeText(text);
  if (/\ba4\b/.test(normalized)) {
    return "A4";
  }
  if (/\ba5\b/.test(normalized)) {
    return "A5";
  }
  return null;
}

export function detectRuling(text: string): string | null {
  const normalized = normalizeText(text);
  if (/(quadriculado|quadriculada|grid|milimetrado)/.test(normalized)) {
    return "quadriculado";
  }
  if (/(pautado|pautada|linhado|linhada|ruled)/.test(normalized)) {
    return "pautado";
  }
  if (/(liso|sem pauta|blank)/.test(normalized)) {
    return "liso";
  }
  return null;
}

export function detectPackCount(text: string): number | null {
  const normalized = normalizeText(text);
  const packMatch = normalized.match(/(?:pack|caixa|conjunto|kit)\s*(?:de\s*)?(\d{1,3})/);
  if (packMatch) {
    return Number(packMatch[1]);
  }

  const xMatch = normalized.match(/\b(\d{1,3})\s*(?:un|unid|unidades|pcs|pecas)\b/);
  if (xMatch) {
    return Number(xMatch[1]);
  }

  return null;
}

export function detectNumericQuantity(text: string): number | null {
  const normalized = normalizeText(text);
  const quantityMatch = normalized.match(/\b(\d{1,3})\s*(?:x|un|unid|unidades|pcs|pecas)\b/);
  if (quantityMatch) {
    return Number(quantityMatch[1]);
  }

  return null;
}

export function safeJsonString(value: unknown): string {
  return JSON.stringify(value).replace(/\n/g, " ");
}

export function trimToEmpty(value: string | undefined): string {
  return value?.trim() ?? "";
}

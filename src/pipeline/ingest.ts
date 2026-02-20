import { readFile } from "node:fs/promises";
import path from "node:path";
import { parse } from "csv-parse/sync";
import XLSX from "xlsx";
import type { NormalizedCatalogProduct, RawCatalogRow } from "../types.js";
import { normalizeText, trimToEmpty } from "../utils/text.js";

const COLUMN_ALIASES: Record<keyof RawCatalogRow, string[]> = {
  sourceSku: ["source_sku", "sku", "codigo", "codigo_sku", "product_sku", "ref"],
  title: ["title", "nome", "name", "product_name", "titulo"],
  description: ["description", "descricao", "descrição", "desc", "detalhes"],
  brand: ["brand", "marca"],
  price: ["price", "preco", "preço", "valor"],
  availability: ["availability", "disponibilidade", "stock", "status"],
  url: ["url", "link", "product_url"],
  imageUrl: ["image_url", "imagem", "image", "foto", "photo_url"],
};

function normalizeHeader(header: string): string {
  return normalizeText(header).replace(/\s+/g, "_");
}

function pickColumn(
  row: Record<string, unknown>,
  aliases: string[],
): string | undefined {
  const normalizedMap = new Map<string, string>();
  for (const key of Object.keys(row)) {
    normalizedMap.set(normalizeHeader(key), key);
  }

  for (const alias of aliases) {
    const realKey = normalizedMap.get(alias);
    if (realKey) {
      return realKey;
    }
  }

  return undefined;
}

function toNumber(value: unknown): number | undefined {
  if (value === null || value === undefined || value === "") {
    return undefined;
  }

  const cleaned = String(value).replace(/[^0-9,.-]/g, "").replace(",", ".");
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function mapRowsToCatalog(rows: Record<string, unknown>[]): RawCatalogRow[] {
  if (rows.length === 0) {
    return [];
  }

  const first = rows[0];
  const keyMap: Partial<Record<keyof RawCatalogRow, string>> = {};

  for (const field of Object.keys(COLUMN_ALIASES) as Array<keyof RawCatalogRow>) {
    keyMap[field] = pickColumn(first, COLUMN_ALIASES[field]);
  }

  if (!keyMap.sourceSku || !keyMap.title) {
    throw new Error(
      "Input file must include columns for source_sku (or alias) and title (or alias).",
    );
  }

  const mapped: RawCatalogRow[] = [];

  for (const row of rows) {
    const sourceSku = trimToEmpty(String(row[keyMap.sourceSku] ?? ""));
    const title = trimToEmpty(String(row[keyMap.title] ?? ""));

    if (!sourceSku || !title) {
      continue;
    }

    mapped.push({
      sourceSku,
      title,
      description: keyMap.description
        ? trimToEmpty(String(row[keyMap.description] ?? "")) || undefined
        : undefined,
      brand: keyMap.brand
        ? trimToEmpty(String(row[keyMap.brand] ?? "")) || undefined
        : undefined,
      price: keyMap.price ? toNumber(row[keyMap.price]) : undefined,
      availability: keyMap.availability
        ? trimToEmpty(String(row[keyMap.availability] ?? "")) || undefined
        : undefined,
      url: keyMap.url ? trimToEmpty(String(row[keyMap.url] ?? "")) || undefined : undefined,
      imageUrl: keyMap.imageUrl
        ? trimToEmpty(String(row[keyMap.imageUrl] ?? "")) || undefined
        : undefined,
    });
  }

  return mapped;
}

export async function readCatalogFile(filePath: string): Promise<RawCatalogRow[]> {
  const extension = path.extname(filePath).toLowerCase();

  if (extension === ".csv") {
    const content = await readFile(filePath, "utf8");
    const rows = parse(content, {
      columns: true,
      skip_empty_lines: true,
      bom: true,
      trim: true,
    }) as Record<string, unknown>[];
    return mapRowsToCatalog(rows);
  }

  if (extension === ".xlsx" || extension === ".xls") {
    const workbook = XLSX.readFile(filePath);
    const firstSheetName = workbook.SheetNames[0];
    if (!firstSheetName) {
      return [];
    }

    const worksheet = workbook.Sheets[firstSheetName];
    const rows = XLSX.utils.sheet_to_json<Record<string, unknown>>(worksheet, {
      defval: "",
    });

    return mapRowsToCatalog(rows);
  }

  throw new Error(`Unsupported input format: ${extension}. Use .csv or .xlsx.`);
}

function rowCompleteness(row: RawCatalogRow): number {
  let score = 0;
  if (row.title) score += 1;
  if (row.description) score += 1;
  if (row.brand) score += 1;
  if (row.price !== undefined) score += 1;
  if (row.availability) score += 1;
  if (row.url) score += 1;
  if (row.imageUrl) score += 1;
  return score;
}

export function deduplicateRows(rows: RawCatalogRow[]): RawCatalogRow[] {
  const bySku = new Map<string, RawCatalogRow>();

  for (const row of rows) {
    const existing = bySku.get(row.sourceSku);
    if (!existing || rowCompleteness(row) > rowCompleteness(existing)) {
      bySku.set(row.sourceSku, row);
    }
  }

  return [...bySku.values()];
}

export function normalizeRows(rows: RawCatalogRow[]): NormalizedCatalogProduct[] {
  return rows.map((row) => {
    const normalizedTitle = normalizeText(row.title);
    const normalizedDescription = normalizeText(row.description);
    const normalizedBrand = normalizeText(row.brand);

    return {
      ...row,
      normalizedTitle,
      normalizedDescription,
      normalizedBrand,
      normalizedText: [normalizedTitle, normalizedDescription, normalizedBrand]
        .filter(Boolean)
        .join(" ")
        .trim(),
    };
  });
}

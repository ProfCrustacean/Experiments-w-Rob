import { mkdtemp, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import XLSX from "xlsx";
import { describe, expect, it } from "vitest";
import { deduplicateRows, normalizeRows, readCatalogFile } from "../src/pipeline/ingest.js";

describe("ingest", () => {
  it("parses CSV and deduplicates by source_sku", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "catalog-csv-"));
    const filePath = path.join(dir, "catalog.csv");

    await writeFile(
      filePath,
      [
        "source_sku,title,description,brand,price",
        "sku-1,Caderno A4 Pautado,Caderno 96 folhas,BrandX,4.99",
        "sku-1,Caderno A4 Pautado,,,",
        "sku-2,Caneta Gel Azul,,BrandY,1.20",
      ].join("\n"),
      "utf8",
    );

    const rows = await readCatalogFile(filePath);
    expect(rows).toHaveLength(3);

    const uniqueRows = deduplicateRows(rows);
    expect(uniqueRows).toHaveLength(2);

    const sku1 = uniqueRows.find((row) => row.sourceSku === "sku-1");
    expect(sku1?.description).toBe("Caderno 96 folhas");
  });

  it("parses XLSX with minimal required fields", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "catalog-xlsx-"));
    const filePath = path.join(dir, "catalog.xlsx");

    const data = [
      { source_sku: "sku-10", title: "Lapis HB" },
      { source_sku: "sku-11", title: "Borracha Branca", descricao: "vinil" },
    ];

    const worksheet = XLSX.utils.json_to_sheet(data);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Sheet1");
    XLSX.writeFile(workbook, filePath);

    const rows = await readCatalogFile(filePath);
    expect(rows).toHaveLength(2);

    const normalized = normalizeRows(rows);
    expect(normalized[0].normalizedText).toContain("lapis hb");
  });
});

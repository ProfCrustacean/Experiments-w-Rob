import path from "node:path";
import { readFile } from "node:fs/promises";
import { parse } from "csv-parse/sync";
import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";
import { upsertQaFeedbackRows, type QaFeedbackImportRow } from "../pipeline/persist.js";
import { parseArgs, requireArg } from "../utils/cli.js";

function parseCorrectedAttributes(raw: string | undefined): Record<string, unknown> {
  if (!raw || raw.trim().length === 0) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
  } catch {
    return {};
  }

  return {};
}

function parseStatus(raw: string | undefined): "pass" | "fail" | "skip" {
  const normalized = raw?.trim().toLowerCase();
  if (normalized === "pass" || normalized === "fail" || normalized === "skip") {
    return normalized;
  }
  return "skip";
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const file = requireArg(args, "file");

  await runMigrations();

  const content = await readFile(path.resolve(process.cwd(), file), "utf8");
  const rows = parse(content, {
    columns: true,
    skip_empty_lines: true,
    bom: true,
    trim: true,
  }) as Array<Record<string, string>>;

  const importRows: QaFeedbackImportRow[] = [];
  for (const row of rows) {
    const runId = row.run_id?.trim();
    const sourceSku = row.source_sku?.trim();
    const predictedCategory = row.predicted_category?.trim();

    if (!runId || !sourceSku || !predictedCategory) {
      continue;
    }

    importRows.push({
      runId,
      sourceSku,
      predictedCategory,
      correctedCategory: row.corrected_category?.trim() ? row.corrected_category.trim() : null,
      correctedAttributesJson: parseCorrectedAttributes(row.corrected_attributes_json),
      reviewStatus: parseStatus(row.review_status),
      reviewNotes: row.review_notes?.trim() ? row.review_notes.trim() : null,
    });
  }

  const upserted = await upsertQaFeedbackRows(importRows);

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        file: path.resolve(process.cwd(), file),
        parsedRows: rows.length,
        importedRows: importRows.length,
        upsertedRows: upserted,
      },
      null,
      2,
    ),
  );
}

main()
  .then(async () => {
    await closePool();
  })
  .catch(async (error: unknown) => {
    // eslint-disable-next-line no-console
    console.error("QA import failed:", error);
    await closePool();
    process.exitCode = 1;
  });

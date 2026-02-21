import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { evaluateQAReport, writeQAReport } from "../src/pipeline/qa-report.js";

describe("qa report", () => {
  it("computes pass rate from reviewed rows", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "qa-report-"));
    const filePath = path.join(dir, "qa.csv");

    await writeFile(
      filePath,
      [
        "run_id,source_sku,title,predicted_category,needs_review,key_attributes,review_status,review_notes",
        "run-1,sku-1,Prod1,caderno,false,{},pass,ok",
        "run-1,sku-2,Prod2,caneta,true,{},fail,errado",
        "run-1,sku-3,Prod3,lapis,false,{},,",
      ].join("\n"),
      "utf8",
    );

    const result = await evaluateQAReport(filePath);
    expect(result.runId).toBe("run-1");
    expect(result.reviewedRows).toBe(2);
    expect(result.passRows).toBe(1);
    expect(result.passRate).toBe(0.5);
  });

  it("writes QA CSV with automation review columns", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "qa-report-write-"));
    const output = await writeQAReport({
      outputDir: dir,
      runId: "run-2",
      sampleSize: 10,
      rows: [
        {
          sourceSku: "sku-1",
          title: "Caderno A4",
          predictedCategory: "cadernos_blocos",
          predictedCategoryConfidence: 0.83,
          predictedCategoryMargin: 0.22,
          autoDecision: "auto",
          topConfidenceReasons: ["strong_lexical_match", "strong_semantic_match"],
          needsReview: false,
          variantSignature: "item_subtype=caderno | format=A4 | ruling=pautado",
          legacySplitHint: "caderno-a4-pautado",
          attributeValues: { format: "A4" },
        },
      ],
    });

    const content = await readFile(output.filePath, "utf8");
    expect(content).toContain("predicted_category_confidence");
    expect(content).toContain("predicted_category_margin");
    expect(content).toContain("auto_decision");
    expect(content).toContain("top_confidence_reasons");
    expect(content).toContain("variant_signature");
    expect(content).toContain("legacy_split_hint");
    expect(content).toContain("corrected_category");
    expect(content).toContain("corrected_attributes_json");
  });

  it("samples QA rows in a stratified way across families", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "qa-report-stratified-"));
    const output = await writeQAReport({
      outputDir: dir,
      runId: "run-3",
      sampleSize: 6,
      rows: [
        ...Array.from({ length: 8 }).map((_, index) => ({
          sourceSku: `escrita-${index}`,
          title: `Caneta ${index}`,
          predictedCategory: "escrita",
          predictedCategoryConfidence: 0.8,
          predictedCategoryMargin: 0.2,
          autoDecision: "auto" as const,
          topConfidenceReasons: ["family_assignment"],
          needsReview: false,
          variantSignature: "",
          legacySplitHint: "",
          attributeValues: {},
        })),
        ...Array.from({ length: 5 }).map((_, index) => ({
          sourceSku: `papel-${index}`,
          title: `Resma ${index}`,
          predictedCategory: "papel",
          predictedCategoryConfidence: 0.8,
          predictedCategoryMargin: 0.2,
          autoDecision: "auto" as const,
          topConfidenceReasons: ["family_assignment"],
          needsReview: false,
          variantSignature: "",
          legacySplitHint: "",
          attributeValues: {},
        })),
        ...Array.from({ length: 4 }).map((_, index) => ({
          sourceSku: `transporte-${index}`,
          title: `Mochila ${index}`,
          predictedCategory: "transporte_escolar",
          predictedCategoryConfidence: 0.8,
          predictedCategoryMargin: 0.2,
          autoDecision: "auto" as const,
          topConfidenceReasons: ["family_assignment"],
          needsReview: false,
          variantSignature: "",
          legacySplitHint: "",
          attributeValues: {},
        })),
      ],
    });

    const csv = await readFile(output.filePath, "utf8");
    const lines = csv.split("\n").slice(1).filter(Boolean);
    const categories = lines.map((line) => line.split(",")[3]);
    expect(categories).toContain("escrita");
    expect(categories).toContain("papel");
    expect(categories).toContain("transporte_escolar");
  });
});

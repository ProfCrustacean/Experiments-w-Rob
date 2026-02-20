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
          predictedCategory: "caderno-a4-pautado",
          predictedCategoryConfidence: 0.83,
          predictedCategoryMargin: 0.22,
          autoDecision: "auto",
          topConfidenceReasons: ["strong_lexical_match", "strong_semantic_match"],
          needsReview: false,
          attributeValues: { format: "A4" },
        },
      ],
    });

    const content = await readFile(output.filePath, "utf8");
    expect(content).toContain("predicted_category_confidence");
    expect(content).toContain("predicted_category_margin");
    expect(content).toContain("auto_decision");
    expect(content).toContain("top_confidence_reasons");
    expect(content).toContain("corrected_category");
    expect(content).toContain("corrected_attributes_json");
  });
});

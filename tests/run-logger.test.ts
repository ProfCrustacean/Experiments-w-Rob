import { describe, expect, it } from "vitest";
import { RunLogger } from "../src/logging/run-logger.js";
import type { PipelineRunLogRow } from "../src/types.js";

describe("run logger", () => {
  it("emits JSON lines with required fields", async () => {
    const lines: string[] = [];
    const inserted: PipelineRunLogRow[] = [];

    const logger = new RunLogger({
      runId: "run-1",
      traceRetentionHours: 24,
      flushBatchSize: 25,
      insertBatch: async (rows) => {
        inserted.push(...rows);
      },
      consoleWrite: (line) => lines.push(line),
      now: () => new Date("2026-02-20T21:00:00.000Z"),
    });

    logger.info("pipeline", "run.started", "Pipeline started.", { foo: "bar" });
    await logger.flush("test");

    const parsed = JSON.parse(lines[0] ?? "{}");
    expect(parsed).toMatchObject({
      run_id: "run-1",
      seq: 1,
      level: "info",
      stage: "pipeline",
      event: "run.started",
      message: "Pipeline started.",
    });
    expect(typeof parsed.timestamp).toBe("string");
    expect(parsed.payload).toEqual({ foo: "bar" });

    expect(inserted.some((row) => row.event === "run.started")).toBe(true);
  });

  it("flushes buffered rows and preserves ordering", async () => {
    const inserted: PipelineRunLogRow[] = [];

    const logger = new RunLogger({
      runId: "run-2",
      traceRetentionHours: 24,
      flushBatchSize: 2,
      insertBatch: async (rows) => {
        inserted.push(...rows);
      },
      consoleWrite: () => {
        // ignore
      },
    });

    logger.info("pipeline", "event.1", "first");
    logger.info("pipeline", "event.2", "second");
    logger.info("pipeline", "event.3", "third");

    await logger.flush("final");

    const eventRows = inserted.filter((row) => row.event.startsWith("event."));
    expect(eventRows.map((row) => row.event)).toEqual(["event.1", "event.2", "event.3"]);
  });

  it("is resilient when DB flush fails", async () => {
    const logger = new RunLogger({
      runId: "run-3",
      traceRetentionHours: 24,
      flushBatchSize: 25,
      insertBatch: async () => {
        throw new Error("db_down");
      },
      consoleWrite: () => {
        // ignore
      },
    });

    logger.error("pipeline", "run.failed", "Pipeline failed.");
    await logger.flush("failure");

    const stats = logger.getStats();
    expect(stats.trace_flush_error_count).toBeGreaterThan(0);
    expect(stats.trace_event_count).toBeGreaterThan(0);
  });

  it("truncates oversized payloads to avoid logger-induced stalls", async () => {
    const inserted: PipelineRunLogRow[] = [];

    const logger = new RunLogger({
      runId: "run-4",
      traceRetentionHours: 24,
      flushBatchSize: 25,
      insertBatch: async (rows) => {
        inserted.push(...rows);
      },
      consoleWrite: () => {
        // ignore
      },
    });

    logger.info("openai", "openai.call.started", "Large payload test", {
      request_body: {
        input: new Array(80).fill("x".repeat(1200)),
      },
    });

    await logger.flush("test_large_payload");

    const event = inserted.find((row) => row.event === "openai.call.started");
    expect(event).toBeDefined();
    expect(event?.payload.__payload_truncated).toBe(true);
    expect(typeof event?.payload.__original_size_bytes).toBe("number");
  });
});

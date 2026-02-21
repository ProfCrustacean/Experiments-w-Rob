import type { PipelineRunLogRow, RunLogLevel } from "../types.js";

export interface RunLoggerStats {
  trace_event_count: number;
  trace_openai_event_count: number;
  trace_flush_error_count: number;
}

interface RunLoggerOptions {
  runId: string;
  traceRetentionHours: number;
  flushBatchSize: number;
  insertBatch: (rows: PipelineRunLogRow[]) => Promise<void>;
  consoleWrite?: (line: string) => void;
  now?: () => Date;
}

const MAX_LOG_PAYLOAD_BYTES = 24_000;
const MAX_LOG_PAYLOAD_DEPTH = 4;
const MAX_LOG_ARRAY_ITEMS = 20;
const MAX_LOG_OBJECT_KEYS = 40;
const MAX_LOG_STRING_CHARS = 700;

function truncateString(value: string): string {
  if (value.length <= MAX_LOG_STRING_CHARS) {
    return value;
  }

  return `${value.slice(0, MAX_LOG_STRING_CHARS)}…`;
}

function truncateForLog(value: unknown, depth: number): unknown {
  if (depth >= MAX_LOG_PAYLOAD_DEPTH) {
    return "[truncated_depth_limit]";
  }

  if (typeof value === "string") {
    return truncateString(value);
  }

  if (typeof value === "number" || typeof value === "boolean" || value === null) {
    return value;
  }

  if (Array.isArray(value)) {
    const truncated = value.slice(0, MAX_LOG_ARRAY_ITEMS).map((entry) => truncateForLog(entry, depth + 1));
    if (value.length > MAX_LOG_ARRAY_ITEMS) {
      truncated.push(`[truncated_items:${value.length - MAX_LOG_ARRAY_ITEMS}]`);
    }
    return truncated;
  }

  if (typeof value === "object" && value !== null) {
    const entries = Object.entries(value as Record<string, unknown>).slice(0, MAX_LOG_OBJECT_KEYS);
    const output: Record<string, unknown> = {};
    for (const [key, entry] of entries) {
      output[key] = truncateForLog(entry, depth + 1);
    }
    const originalKeyCount = Object.keys(value as Record<string, unknown>).length;
    if (originalKeyCount > MAX_LOG_OBJECT_KEYS) {
      output.__truncated_keys = originalKeyCount - MAX_LOG_OBJECT_KEYS;
    }
    return output;
  }

  return String(value);
}

function safePayload(value: unknown): Record<string, unknown> {
  if (value === undefined || value === null) {
    return {};
  }

  try {
    const parsed = JSON.parse(JSON.stringify(value));
    const normalized =
      parsed && typeof parsed === "object" && !Array.isArray(parsed)
        ? (parsed as Record<string, unknown>)
        : { value: parsed as unknown };

    const serialized = JSON.stringify(normalized);
    if (Buffer.byteLength(serialized, "utf8") <= MAX_LOG_PAYLOAD_BYTES) {
      return normalized;
    }

    const truncatedPayload = truncateForLog(normalized, 0);
    if (truncatedPayload && typeof truncatedPayload === "object" && !Array.isArray(truncatedPayload)) {
      return {
        __payload_truncated: true,
        __original_size_bytes: Buffer.byteLength(serialized, "utf8"),
        ...(truncatedPayload as Record<string, unknown>),
      };
    }

    return {
      __payload_truncated: true,
      __original_size_bytes: Buffer.byteLength(serialized, "utf8"),
      value: truncatedPayload,
    };
  } catch {
    return { payload_serialization_error: true };
  }
}

export class RunLogger {
  private readonly runId: string;
  private readonly traceRetentionHours: number;
  private readonly flushBatchSize: number;
  private readonly insertBatch: (rows: PipelineRunLogRow[]) => Promise<void>;
  private readonly consoleWrite: (line: string) => void;
  private readonly now: () => Date;

  private readonly buffer: PipelineRunLogRow[] = [];
  private nextSeq = 1;
  private flushChain: Promise<void> = Promise.resolve();
  private isFlushing = false;

  private readonly stats: RunLoggerStats = {
    trace_event_count: 0,
    trace_openai_event_count: 0,
    trace_flush_error_count: 0,
  };

  constructor(options: RunLoggerOptions) {
    this.runId = options.runId;
    this.traceRetentionHours = options.traceRetentionHours;
    this.flushBatchSize = options.flushBatchSize;
    this.insertBatch = options.insertBatch;
    this.consoleWrite = options.consoleWrite ?? ((line) => console.log(line));
    this.now = options.now ?? (() => new Date());
  }

  getStats(): RunLoggerStats {
    return { ...this.stats };
  }

  log(
    level: RunLogLevel,
    stage: string,
    event: string,
    message: string,
    payload?: Record<string, unknown>,
  ): void {
    const row = this.createRow(level, stage, event, message, payload);
    this.writeLine(row);
    this.buffer.push(row);

    if (this.buffer.length >= this.flushBatchSize && !this.isFlushing) {
      this.scheduleFlush("threshold");
    }
  }

  debug(stage: string, event: string, message: string, payload?: Record<string, unknown>): void {
    this.log("debug", stage, event, message, payload);
  }

  info(stage: string, event: string, message: string, payload?: Record<string, unknown>): void {
    this.log("info", stage, event, message, payload);
  }

  warn(stage: string, event: string, message: string, payload?: Record<string, unknown>): void {
    this.log("warn", stage, event, message, payload);
  }

  error(stage: string, event: string, message: string, payload?: Record<string, unknown>): void {
    this.log("error", stage, event, message, payload);
  }

  async flush(reason = "manual"): Promise<void> {
    this.scheduleFlush(reason);
    await this.flushChain;

    while (this.buffer.length > 0) {
      this.scheduleFlush(`${reason}_drain`);
      await this.flushChain;
    }
  }

  private scheduleFlush(reason: string): void {
    this.flushChain = this.flushChain
      .then(async () => {
        await this.flushInternal(reason);
      })
      .catch(() => {
        // Best-effort logger should never break pipeline execution.
      });
  }

  private createRow(
    level: RunLogLevel,
    stage: string,
    event: string,
    message: string,
    payload?: Record<string, unknown>,
  ): PipelineRunLogRow {
    const createdAt = this.now();
    const expiresAt = new Date(createdAt.getTime() + this.traceRetentionHours * 60 * 60 * 1000);

    this.stats.trace_event_count += 1;
    if (event.startsWith("openai.")) {
      this.stats.trace_openai_event_count += 1;
    }

    return {
      runId: this.runId,
      seq: this.nextSeq++,
      level,
      stage,
      event,
      message,
      payload: safePayload(payload),
      timestamp: createdAt.toISOString(),
      expiresAt: expiresAt.toISOString(),
    };
  }

  private writeLine(row: PipelineRunLogRow): void {
    this.consoleWrite(
      JSON.stringify({
        timestamp: row.timestamp,
        run_id: row.runId,
        seq: row.seq,
        level: row.level,
        stage: row.stage,
        event: row.event,
        message: row.message,
        payload: row.payload,
      }),
    );
  }

  private async flushInternal(reason: string): Promise<void> {
    if (this.isFlushing || this.buffer.length === 0) {
      return;
    }

    this.isFlushing = true;
    const rows = this.buffer.splice(0, this.buffer.length);

    const startedRow = this.createRow(
      "debug",
      "persistence",
      "db.flush.started",
      "Flushing buffered run logs to database.",
      {
        reason,
        row_count: rows.length,
      },
    );
    this.writeLine(startedRow);

    try {
      await this.insertBatch([startedRow, ...rows]);

      const completedRow = this.createRow(
        "debug",
        "persistence",
        "db.flush.completed",
        "Buffered run logs were flushed.",
        {
          reason,
          row_count: rows.length,
        },
      );
      this.writeLine(completedRow);
      await this.insertBatch([completedRow]);
    } catch (error) {
      this.stats.trace_flush_error_count += 1;

      const failedRow = this.createRow(
        "error",
        "persistence",
        "db.flush.failed",
        "Failed to flush buffered run logs; continuing execution.",
        {
          reason,
          row_count: rows.length,
          error_message: error instanceof Error ? error.message : "unknown_error",
        },
      );
      this.writeLine(failedRow);

      try {
        await this.insertBatch([failedRow]);
      } catch {
        // Ignore nested logger storage failures.
      }
    } finally {
      this.isFlushing = false;
      if (this.buffer.length >= this.flushBatchSize) {
        this.scheduleFlush("post_flush_threshold");
      }
    }
  }
}

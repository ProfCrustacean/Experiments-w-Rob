import type { RawCatalogRow, NormalizedCatalogProduct } from "../types.js";
import { deduplicateRows, normalizeRows, readCatalogFile } from "./ingest.js";
import { partitionProductsBySample } from "./run-support.js";
import { RunLogger } from "../logging/run-logger.js";

export interface IngestSamplingResult {
  sourceRows: RawCatalogRow[];
  deduplicatedRows: RawCatalogRow[];
  normalizedRows: NormalizedCatalogProduct[];
  partitioned: {
    sampled: NormalizedCatalogProduct[];
    skipped: number;
  };
  ingestElapsedMs: number;
  samplingElapsedMs: number;
}

export async function runIngestAndSampling(input: {
  inputPath: string;
  storeId: string;
  sampleParts: number;
  samplePartIndex: number;
}): Promise<IngestSamplingResult> {
  const ingestStart = Date.now();
  const sourceRows = await readCatalogFile(input.inputPath);
  const deduplicatedRows = deduplicateRows(sourceRows);
  const normalizedRows = normalizeRows(deduplicatedRows);
  const ingestElapsedMs = Date.now() - ingestStart;

  if (normalizedRows.length === 0) {
    throw new Error("No valid products found in the input file.");
  }

  const samplingStart = Date.now();
  const partitioned = partitionProductsBySample(
    normalizedRows,
    input.storeId,
    input.sampleParts,
    input.samplePartIndex,
  );
  const samplingElapsedMs = Date.now() - samplingStart;

  if (partitioned.sampled.length === 0) {
    throw new Error(
      `Sampling selected 0 products (sample_parts=${input.sampleParts}, sample_part_index=${input.samplePartIndex}).`,
    );
  }

  // eslint-disable-next-line no-console
  console.log(
    `[sampling] selected ${partitioned.sampled.length}/${normalizedRows.length} unique products (sample_part_index=${input.samplePartIndex}, sample_parts=${input.sampleParts})`,
  );

  return {
    sourceRows,
    deduplicatedRows,
    normalizedRows,
    partitioned,
    ingestElapsedMs,
    samplingElapsedMs,
  };
}

export function logHistoricalIngestAndSampling(input: {
  logger: RunLogger;
  ingestElapsedMs: number;
  samplingElapsedMs: number;
  sourceRowCount: number;
  deduplicatedRowCount: number;
  normalizedRowCount: number;
  sampledRowCount: number;
  skippedRowCount: number;
}): void {
  input.logger.info("pipeline", "stage.started", "Starting ingest stage.", {
    stage_name: "ingest",
    historical_stage: true,
  });
  input.logger.info("pipeline", "stage.completed", "Ingest stage completed.", {
    stage_name: "ingest",
    historical_stage: true,
    elapsed_ms: input.ingestElapsedMs,
    input_rows: input.sourceRowCount,
    deduplicated_rows: input.deduplicatedRowCount,
    normalized_rows: input.normalizedRowCount,
  });

  input.logger.info("pipeline", "stage.started", "Starting sampling stage.", {
    stage_name: "sampling",
    historical_stage: true,
  });
  input.logger.info("pipeline", "stage.completed", "Sampling stage completed.", {
    stage_name: "sampling",
    historical_stage: true,
    elapsed_ms: input.samplingElapsedMs,
    sampled_rows: input.sampledRowCount,
    skipped_rows: input.skippedRowCount,
  });
}

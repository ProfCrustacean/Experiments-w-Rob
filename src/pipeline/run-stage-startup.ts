import type { AppConfig } from "../config.js";
import type { OpenAITelemetryCallback } from "../services/openai.js";
import { RunLogger } from "../logging/run-logger.js";
import { logHistoricalIngestAndSampling } from "./run-stage-ingest.js";
import { createProviders } from "./run-support.js";

function buildConfigSnapshot(config: AppConfig): Record<string, unknown> {
  return {
    llm_model: config.LLM_MODEL,
    embedding_model: config.EMBEDDING_MODEL,
    confidence_threshold: config.CONFIDENCE_THRESHOLD,
    category_profile_concurrency: config.CATEGORY_PROFILE_CONCURRENCY,
    attribute_batch_size: config.ATTRIBUTE_BATCH_SIZE,
    attribute_llm_concurrency: config.ATTRIBUTE_LLM_CONCURRENCY,
    attribute_second_pass_enabled: config.ATTRIBUTE_SECOND_PASS_ENABLED,
    attribute_second_pass_model: config.ATTRIBUTE_SECOND_PASS_MODEL,
    attribute_second_pass_batch_size: config.ATTRIBUTE_SECOND_PASS_BATCH_SIZE,
    attribute_second_pass_max_products: config.ATTRIBUTE_SECOND_PASS_MAX_PRODUCTS,
    embedding_batch_size: config.EMBEDDING_BATCH_SIZE,
    embedding_concurrency: config.EMBEDDING_CONCURRENCY,
    openai_timeout_ms: config.OPENAI_TIMEOUT_MS,
    openai_max_retries: config.OPENAI_MAX_RETRIES,
    openai_retry_base_ms: config.OPENAI_RETRY_BASE_MS,
    openai_retry_max_ms: config.OPENAI_RETRY_MAX_MS,
    category_auto_min_confidence: config.CATEGORY_AUTO_MIN_CONFIDENCE,
    category_auto_min_margin: config.CATEGORY_AUTO_MIN_MARGIN,
    attribute_auto_min_confidence: config.ATTRIBUTE_AUTO_MIN_CONFIDENCE,
    high_risk_category_extra_confidence: config.HIGH_RISK_CATEGORY_EXTRA_CONFIDENCE,
    quality_qa_sample_size: config.QUALITY_QA_SAMPLE_SIZE,
    trace_retention_hours: config.TRACE_RETENTION_HOURS,
    trace_flush_batch_size: config.TRACE_FLUSH_BATCH_SIZE,
    product_persist_stage_timeout_ms: config.PRODUCT_PERSIST_STAGE_TIMEOUT_MS,
    product_vector_query_timeout_ms: config.PRODUCT_VECTOR_QUERY_TIMEOUT_MS,
    product_vector_batch_size: config.PRODUCT_VECTOR_BATCH_SIZE,
    output_dir: config.OUTPUT_DIR,
  };
}

export function runStartupStage(input: {
  runId: string;
  storeId: string;
  inputFileName: string;
  sampleParts: number;
  samplePartIndex: number;
  sourceRowCount: number;
  deduplicatedRowCount: number;
  normalizedRowCount: number;
  sampledRowCount: number;
  skippedRowCount: number;
  ingestElapsedMs: number;
  samplingElapsedMs: number;
  config: AppConfig;
  logger: RunLogger;
}): ReturnType<typeof createProviders> {
  if (!input.config.OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY is missing. OpenAI mode is required for this pipeline run.");
  }

  input.logger.info("pipeline", "run.started", "Pipeline run started.", {
    run_id: input.runId,
    store_id: input.storeId,
    input_file_name: input.inputFileName,
    sample_parts: input.sampleParts,
    sample_part_index: input.samplePartIndex,
    openai_enabled: true,
    config: buildConfigSnapshot(input.config),
  });

  logHistoricalIngestAndSampling({
    logger: input.logger,
    ingestElapsedMs: input.ingestElapsedMs,
    samplingElapsedMs: input.samplingElapsedMs,
    sourceRowCount: input.sourceRowCount,
    deduplicatedRowCount: input.deduplicatedRowCount,
    normalizedRowCount: input.normalizedRowCount,
    sampledRowCount: input.sampledRowCount,
    skippedRowCount: input.skippedRowCount,
  });

  const openAITelemetry: OpenAITelemetryCallback = (event) => {
    input.logger.log(event.level, event.stage, event.event, event.message, event.payload);
  };

  return createProviders(openAITelemetry);
}

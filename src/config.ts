import "dotenv/config";
import { z } from "zod";

const envSchema = z.object({
  OPENAI_API_KEY: z.string().min(1).optional(),
  DATABASE_URL: z.string().min(1, "DATABASE_URL is required"),
  CATALOG_INPUT_PATH: z.string().min(1).optional(),
  STORE_ID: z.string().min(1).optional(),
  LLM_MODEL: z.string().default("gpt-4.1-mini"),
  EMBEDDING_MODEL: z.string().default("text-embedding-3-large"),
  CONFIDENCE_THRESHOLD: z.coerce.number().min(0).max(1).default(0.7),
  BATCH_SIZE: z.coerce.number().int().positive().default(25),
  CONCURRENCY: z.coerce.number().int().positive().default(5),
  INPUT_SAMPLE_PARTS: z.coerce.number().int().positive().default(1),
  INPUT_SAMPLE_PART_INDEX: z.coerce.number().int().nonnegative().default(0),
  CATEGORY_PROFILE_CONCURRENCY: z.coerce.number().int().positive().default(8),
  ATTRIBUTE_BATCH_SIZE: z.coerce.number().int().positive().default(8),
  ATTRIBUTE_LLM_CONCURRENCY: z.coerce.number().int().positive().default(8),
  EMBEDDING_BATCH_SIZE: z.coerce.number().int().positive().default(50),
  EMBEDDING_CONCURRENCY: z.coerce.number().int().positive().default(10),
  OPENAI_TIMEOUT_MS: z.coerce.number().int().positive().default(25_000),
  OPENAI_MAX_RETRIES: z.coerce.number().int().nonnegative().default(3),
  OPENAI_RETRY_BASE_MS: z.coerce.number().int().positive().default(750),
  OPENAI_RETRY_MAX_MS: z.coerce.number().int().positive().default(6_000),
  CATEGORY_AUTO_MIN_CONFIDENCE: z.coerce.number().min(0).max(1).default(0.76),
  CATEGORY_AUTO_MIN_MARGIN: z.coerce.number().min(0).max(1).default(0.1),
  ATTRIBUTE_AUTO_MIN_CONFIDENCE: z.coerce.number().min(0).max(1).default(0.7),
  HIGH_RISK_CATEGORY_EXTRA_CONFIDENCE: z.coerce.number().min(0).max(1).default(0.08),
  QUALITY_QA_SAMPLE_SIZE: z.coerce.number().int().positive().default(250),
  QA_SAMPLE_SIZE: z.coerce.number().int().positive().default(200),
  ARTIFACT_RETENTION_HOURS: z.coerce.number().int().positive().default(24),
  TRACE_RETENTION_HOURS: z.coerce.number().int().positive().default(24),
  TRACE_FLUSH_BATCH_SIZE: z.coerce.number().int().positive().default(25),
  STALE_RUN_TIMEOUT_MINUTES: z.coerce.number().int().positive().default(180),
  PRODUCT_PERSIST_STAGE_TIMEOUT_MS: z.coerce.number().int().positive().default(60_000),
  PRODUCT_VECTOR_QUERY_TIMEOUT_MS: z.coerce.number().int().positive().default(20_000),
  PRODUCT_VECTOR_BATCH_SIZE: z.coerce.number().int().positive().default(100),
  OUTPUT_DIR: z.string().min(1).default("outputs"),
  CANARY_SAMPLE_SIZE: z.coerce.number().int().positive().default(350),
  CANARY_FIXED_RATIO: z.coerce.number().min(0).max(1).default(0.3),
  CANARY_RANDOM_SEED: z.string().min(1).default("canary-v1"),
  CANARY_AUTO_ACCEPT_THRESHOLD: z.coerce.number().min(0).max(1).default(0.8),
  CANARY_SUBSET_PATH: z.string().min(1).default("outputs/canary_input.csv"),
  CANARY_STATE_PATH: z.string().min(1).default("outputs/canary_state.json"),
  SELF_IMPROVE_MAX_LOOPS: z.coerce.number().int().positive().default(10),
  SELF_IMPROVE_RETRY_LIMIT: z.coerce.number().int().nonnegative().default(1),
  SELF_IMPROVE_AUTO_APPLY_POLICY: z.enum(["if_gate_passes"]).default("if_gate_passes"),
  SELF_IMPROVE_WORKER_POLL_MS: z.coerce.number().int().positive().default(5000),
  SELF_IMPROVE_STALE_RUN_TIMEOUT_MINUTES: z.coerce.number().int().positive().default(30),
  SELF_IMPROVE_MAX_STRUCTURAL_CHANGES_PER_LOOP: z.coerce.number().int().nonnegative().default(2),
  SELF_IMPROVE_GATE_MIN_SAMPLE_SIZE: z.coerce.number().int().positive().default(200),
  SELF_IMPROVE_POST_APPLY_WATCH_LOOPS: z.coerce.number().int().positive().default(2),
  SELF_IMPROVE_ROLLBACK_ON_DEGRADE: z.coerce.boolean().default(true),
  SELF_IMPROVE_CANARY_RETRY_DEGRADE_ENABLED: z.coerce.boolean().default(true),
  SELF_IMPROVE_CANARY_RETRY_MIN_PROPOSAL_CONFIDENCE: z.coerce.number().min(0).max(1).default(0.75),
  HARNESS_MIN_L1_DELTA: z.coerce.number().default(0),
  HARNESS_MIN_L2_DELTA: z.coerce.number().default(0),
  HARNESS_MIN_L3_DELTA: z.coerce.number().default(0),
  HARNESS_MAX_FALLBACK_RATE: z.coerce.number().min(0).max(1).default(0.06),
  HARNESS_MAX_NEEDS_REVIEW_RATE: z.coerce.number().min(0).max(1).default(0.35),
});

export type AppConfig = z.infer<typeof envSchema>;

let cachedConfig: AppConfig | null = null;

export function getConfig(): AppConfig {
  if (cachedConfig) {
    return cachedConfig;
  }

  const parsed = envSchema.safeParse(process.env);
  if (!parsed.success) {
    const errors = parsed.error.issues
      .map((issue) => `${issue.path.join(".")}: ${issue.message}`)
      .join("; ");
    throw new Error(`Invalid environment configuration: ${errors}`);
  }

  if (parsed.data.INPUT_SAMPLE_PART_INDEX >= parsed.data.INPUT_SAMPLE_PARTS) {
    throw new Error(
      `Invalid environment configuration: INPUT_SAMPLE_PART_INDEX (${parsed.data.INPUT_SAMPLE_PART_INDEX}) must be smaller than INPUT_SAMPLE_PARTS (${parsed.data.INPUT_SAMPLE_PARTS})`,
    );
  }

  if (parsed.data.OPENAI_RETRY_BASE_MS > parsed.data.OPENAI_RETRY_MAX_MS) {
    throw new Error(
      `Invalid environment configuration: OPENAI_RETRY_BASE_MS (${parsed.data.OPENAI_RETRY_BASE_MS}) must be <= OPENAI_RETRY_MAX_MS (${parsed.data.OPENAI_RETRY_MAX_MS})`,
    );
  }

  if (parsed.data.CATEGORY_AUTO_MIN_MARGIN >= parsed.data.CATEGORY_AUTO_MIN_CONFIDENCE) {
    throw new Error(
      `Invalid environment configuration: CATEGORY_AUTO_MIN_MARGIN (${parsed.data.CATEGORY_AUTO_MIN_MARGIN}) should be lower than CATEGORY_AUTO_MIN_CONFIDENCE (${parsed.data.CATEGORY_AUTO_MIN_CONFIDENCE})`,
    );
  }

  const canaryFixedTarget = Math.round(parsed.data.CANARY_SAMPLE_SIZE * parsed.data.CANARY_FIXED_RATIO);
  if (canaryFixedTarget <= 0) {
    throw new Error(
      `Invalid environment configuration: CANARY_FIXED_RATIO (${parsed.data.CANARY_FIXED_RATIO}) and CANARY_SAMPLE_SIZE (${parsed.data.CANARY_SAMPLE_SIZE}) must produce at least one fixed canary product.`,
    );
  }

  if (parsed.data.SELF_IMPROVE_MAX_LOOPS > 10) {
    throw new Error(
      `Invalid environment configuration: SELF_IMPROVE_MAX_LOOPS (${parsed.data.SELF_IMPROVE_MAX_LOOPS}) must be <= 10.`,
    );
  }

  if (
    parsed.data.SELF_IMPROVE_MAX_STRUCTURAL_CHANGES_PER_LOOP >
    parsed.data.SELF_IMPROVE_MAX_LOOPS
  ) {
    throw new Error(
      `Invalid environment configuration: SELF_IMPROVE_MAX_STRUCTURAL_CHANGES_PER_LOOP (${parsed.data.SELF_IMPROVE_MAX_STRUCTURAL_CHANGES_PER_LOOP}) must be <= SELF_IMPROVE_MAX_LOOPS (${parsed.data.SELF_IMPROVE_MAX_LOOPS}).`,
    );
  }

  if (parsed.data.PRODUCT_VECTOR_QUERY_TIMEOUT_MS > parsed.data.PRODUCT_PERSIST_STAGE_TIMEOUT_MS) {
    throw new Error(
      `Invalid environment configuration: PRODUCT_VECTOR_QUERY_TIMEOUT_MS (${parsed.data.PRODUCT_VECTOR_QUERY_TIMEOUT_MS}) must be <= PRODUCT_PERSIST_STAGE_TIMEOUT_MS (${parsed.data.PRODUCT_PERSIST_STAGE_TIMEOUT_MS}).`,
    );
  }

  cachedConfig = parsed.data;
  return parsed.data;
}

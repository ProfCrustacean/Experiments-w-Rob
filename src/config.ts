import "dotenv/config";
import { z } from "zod";

const envSchema = z.object({
  OPENAI_API_KEY: z.string().min(1).optional(),
  DATABASE_URL: z.string().min(1, "DATABASE_URL is required"),
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
  QA_SAMPLE_SIZE: z.coerce.number().int().positive().default(200),
  ARTIFACT_RETENTION_HOURS: z.coerce.number().int().positive().default(24),
  TRACE_RETENTION_HOURS: z.coerce.number().int().positive().default(24),
  TRACE_FLUSH_BATCH_SIZE: z.coerce.number().int().positive().default(25),
  OUTPUT_DIR: z.string().min(1).default("outputs"),
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

  cachedConfig = parsed.data;
  return parsed.data;
}

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
  QA_SAMPLE_SIZE: z.coerce.number().int().positive().default(200),
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

  cachedConfig = parsed.data;
  return parsed.data;
}

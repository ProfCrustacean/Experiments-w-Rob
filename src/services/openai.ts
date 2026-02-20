import OpenAI from "openai";
import { z } from "zod";
import type {
  AttributeExtractionLLMOutput,
  BatchAttributeExtractionInput,
  CategoryAttribute,
  CategoryProfileLLMOutput,
  EmbeddingProvider,
  LLMProvider,
} from "../types.js";

const categoryProfileSchema = z.object({
  name_pt: z.string().min(2),
  description_pt: z.string().min(10),
  synonyms: z.array(z.string()).default([]),
  attributes: z.array(
    z.object({
      key: z
        .string()
        .regex(/^[a-z0-9_]{2,40}$/)
        .min(2),
      label_pt: z.string().min(2),
      type: z.enum(["enum", "number", "boolean", "text"]),
      allowed_values: z.array(z.string()).optional(),
      required: z.boolean(),
    }),
  ),
});

const extractionSchema = z.object({
  values: z.record(z.string(), z.union([z.string(), z.number(), z.boolean(), z.null()])),
  confidence: z.record(z.string(), z.number().min(0).max(1)),
});

const batchExtractionSchema = z.object({
  results: z.array(
    z.object({
      source_sku: z.string().min(1),
      values: z.record(z.string(), z.union([z.string(), z.number(), z.boolean(), z.null()])),
      confidence: z.record(z.string(), z.number().min(0).max(1)),
    }),
  ),
});

interface OpenAIProviderOptions {
  apiKey: string;
  llmModel: string;
  embeddingModel: string;
  dimensions: number;
  timeoutMs: number;
  maxRetries: number;
  retryBaseMs: number;
  retryMaxMs: number;
}

interface OpenAIProviderStats {
  embedding_call_count: number;
  category_profile_call_count: number;
  attribute_batch_call_count: number;
  retry_count: number;
  timeout_count: number;
  request_failure_count: number;
}

function clampConfidence(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Math.max(0, Math.min(1, parsed));
}

function isTimeoutError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  return error.message.includes("openai_request_timeout");
}

function isRetryableError(error: unknown): boolean {
  if (isTimeoutError(error)) {
    return true;
  }

  if (typeof error === "object" && error !== null) {
    const status = (error as { status?: unknown }).status;
    if (typeof status === "number" && (status === 429 || status >= 500)) {
      return true;
    }

    const code = (error as { code?: unknown }).code;
    if (typeof code === "string" && ["ETIMEDOUT", "ECONNRESET", "ECONNABORTED"].includes(code)) {
      return true;
    }

    const message = (error as { message?: unknown }).message;
    if (typeof message === "string") {
      const normalized = message.toLowerCase();
      if (
        normalized.includes("timeout") ||
        normalized.includes("timed out") ||
        normalized.includes("rate limit") ||
        normalized.includes("too many requests")
      ) {
        return true;
      }
    }
  }

  return false;
}

export class OpenAIProvider implements EmbeddingProvider, LLMProvider {
  public readonly dimensions: number;

  private readonly client: OpenAI;
  private readonly llmModel: string;
  private readonly embeddingModel: string;
  private readonly timeoutMs: number;
  private readonly maxRetries: number;
  private readonly retryBaseMs: number;
  private readonly retryMaxMs: number;

  private readonly stats: OpenAIProviderStats = {
    embedding_call_count: 0,
    category_profile_call_count: 0,
    attribute_batch_call_count: 0,
    retry_count: 0,
    timeout_count: 0,
    request_failure_count: 0,
  };

  constructor(options: OpenAIProviderOptions) {
    this.client = new OpenAI({ apiKey: options.apiKey });
    this.llmModel = options.llmModel;
    this.embeddingModel = options.embeddingModel;
    this.dimensions = options.dimensions;
    this.timeoutMs = options.timeoutMs;
    this.maxRetries = options.maxRetries;
    this.retryBaseMs = options.retryBaseMs;
    this.retryMaxMs = options.retryMaxMs;
  }

  getStats(): OpenAIProviderStats {
    return { ...this.stats };
  }

  private async delay(ms: number): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async withTimeout<T>(operation: Promise<T>): Promise<T> {
    let timeoutHandle: NodeJS.Timeout | undefined;

    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutHandle = setTimeout(() => {
        reject(new Error("openai_request_timeout"));
      }, this.timeoutMs);
    });

    try {
      return await Promise.race([operation, timeoutPromise]);
    } finally {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
    }
  }

  private async withRetry<T>(operation: () => Promise<T>): Promise<T> {
    const totalAttempts = this.maxRetries + 1;

    for (let attempt = 1; attempt <= totalAttempts; attempt += 1) {
      try {
        return await this.withTimeout(operation());
      } catch (error) {
        if (isTimeoutError(error)) {
          this.stats.timeout_count += 1;
        }

        const retryable = isRetryableError(error);
        if (!retryable || attempt === totalAttempts) {
          this.stats.request_failure_count += 1;
          throw error;
        }

        this.stats.retry_count += 1;
        const baseDelay = Math.min(this.retryMaxMs, this.retryBaseMs * 2 ** (attempt - 1));
        const jitteredDelay = Math.round(baseDelay * (0.8 + Math.random() * 0.4));
        await this.delay(jitteredDelay);
      }
    }

    throw new Error("unreachable_retry_state");
  }

  private buildEmptyExtractionOutput(
    attributes: CategoryAttribute[],
  ): AttributeExtractionLLMOutput {
    const values: Record<string, string | number | boolean | null> = {};
    const confidence: Record<string, number> = {};

    for (const attribute of attributes) {
      values[attribute.key] = null;
      confidence[attribute.key] = 0;
    }

    return { values, confidence };
  }

  async embedMany(texts: string[]): Promise<number[][]> {
    if (texts.length === 0) {
      return [];
    }

    this.stats.embedding_call_count += 1;
    const response = await this.withRetry(() =>
      this.client.embeddings.create({
        model: this.embeddingModel,
        input: texts,
        dimensions: this.dimensions,
      }),
    );

    return response.data
      .sort((a, b) => a.index - b.index)
      .map((item) => item.embedding);
  }

  async generateCategoryProfile(input: {
    candidateName: string;
    sampleProducts: Array<{ title: string; description?: string; brand?: string }>;
  }): Promise<CategoryProfileLLMOutput> {
    const prompt = [
      "Voce e um especialista em papelaria escolar para ecommerce em Portugal.",
      "Responda APENAS com JSON valido.",
      "Crie uma categoria curta e especifica em portugues para os produtos abaixo.",
      "A descricao deve detalhar o que pertence e o que nao pertence na categoria.",
      "Os atributos devem ser praticos para comparacao de produto.",
      "Para atributos enum, inclua allowed_values.",
      "candidate_name: " + input.candidateName,
      "sample_products:",
      JSON.stringify(input.sampleProducts, null, 2),
    ].join("\n");

    this.stats.category_profile_call_count += 1;
    const completion = await this.withRetry(() =>
      this.client.chat.completions.create({
        model: this.llmModel,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content:
              "Retorne JSON no formato: {name_pt, description_pt, synonyms, attributes:[{key,label_pt,type,allowed_values?,required}]}",
          },
          {
            role: "user",
            content: prompt,
          },
        ],
        temperature: 0.2,
      }),
    );

    const content = completion.choices[0]?.message?.content ?? "{}";
    const parsed = JSON.parse(content);
    return categoryProfileSchema.parse(parsed);
  }

  async extractProductAttributes(input: {
    product: { title: string; description?: string; brand?: string };
    categoryName: string;
    categoryDescription: string;
    attributeSchema: {
      schema_version: "1.0";
      category_name_pt: string;
      attributes: Array<{
        key: string;
        label_pt: string;
        type: "enum" | "number" | "boolean" | "text";
        allowed_values?: string[];
        required: boolean;
      }>;
    };
  }): Promise<AttributeExtractionLLMOutput> {
    const sku = "single-item";
    const batchOutput = await this.extractProductAttributesBatch({
      categoryName: input.categoryName,
      categoryDescription: input.categoryDescription,
      attributeSchema: input.attributeSchema,
      products: [
        {
          sourceSku: sku,
          product: input.product,
        },
      ],
    });

    return batchOutput[sku] ?? this.buildEmptyExtractionOutput(input.attributeSchema.attributes);
  }

  async extractProductAttributesBatch(
    input: BatchAttributeExtractionInput,
  ): Promise<Record<string, AttributeExtractionLLMOutput>> {
    if (input.products.length === 0) {
      return {};
    }

    const prompt = [
      "Extraia atributos para multiplos produtos com base no schema da categoria.",
      "Responda APENAS com JSON valido.",
      "Nao invente valores; se nao encontrar, use null com baixa confianca.",
      "Use apenas os source_sku fornecidos e apenas chaves do schema.",
      `category_name: ${input.categoryName}`,
      `category_description: ${input.categoryDescription}`,
      `schema: ${JSON.stringify(input.attributeSchema)}`,
      `products: ${JSON.stringify(input.products)}`,
    ].join("\n");

    this.stats.attribute_batch_call_count += 1;
    const completion = await this.withRetry(() =>
      this.client.chat.completions.create({
        model: this.llmModel,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content:
              "Retorne JSON no formato {results:[{source_sku, values:{[key]:valueOuNull}, confidence:{[key]:numero0a1}}]}. Use apenas chaves existentes no schema.",
          },
          {
            role: "user",
            content: prompt,
          },
        ],
        temperature: 0,
      }),
    );

    const content = completion.choices[0]?.message?.content ?? "{}";
    const parsed = JSON.parse(content);
    const structured = batchExtractionSchema.parse(parsed);

    const requestedSkus = new Set(input.products.map((item) => item.sourceSku));
    const parsedBySku = new Map<string, AttributeExtractionLLMOutput>();

    for (const result of structured.results) {
      if (!requestedSkus.has(result.source_sku)) {
        continue;
      }

      parsedBySku.set(result.source_sku, {
        values: result.values,
        confidence: result.confidence,
      });
    }

    const output: Record<string, AttributeExtractionLLMOutput> = {};
    for (const product of input.products) {
      const base = this.buildEmptyExtractionOutput(input.attributeSchema.attributes);
      const extracted = parsedBySku.get(product.sourceSku);

      if (extracted) {
        for (const attribute of input.attributeSchema.attributes) {
          const key = attribute.key;
          if (Object.prototype.hasOwnProperty.call(extracted.values, key)) {
            base.values[key] = extracted.values[key] ?? null;
          }

          if (Object.prototype.hasOwnProperty.call(extracted.confidence, key)) {
            base.confidence[key] = clampConfidence(extracted.confidence[key]);
          }
        }
      }

      output[product.sourceSku] = extractionSchema.parse(base);
    }

    return output;
  }
}

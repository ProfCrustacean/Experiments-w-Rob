import OpenAI from "openai";
import { z } from "zod";
import type {
  AttributeExtractionLLMOutput,
  BatchAttributeExtractionInput,
  CategoryDisambiguationInput,
  CategoryDisambiguationOutput,
  CategoryAttribute,
  CategoryProfileLLMOutput,
  EmbeddingProvider,
  LLMProvider,
  RunLogLevel,
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

const disambiguationSchema = z.object({
  category_slug: z.string().min(1).nullable(),
  confidence: z.number().min(0).max(1),
  reason: z.string().min(1),
});

const MAX_EMBEDDING_INPUT_CHARS = 6000;

interface OpenAIProviderOptions {
  apiKey: string;
  llmModel: string;
  embeddingModel: string;
  dimensions: number;
  timeoutMs: number;
  maxRetries: number;
  retryBaseMs: number;
  retryMaxMs: number;
  telemetry?: OpenAITelemetryCallback;
}

interface OpenAIProviderStats {
  embedding_call_count: number;
  category_profile_call_count: number;
  attribute_batch_call_count: number;
  category_disambiguation_call_count: number;
  retry_count: number;
  timeout_count: number;
  request_failure_count: number;
}

export interface OpenAITelemetryEvent {
  level: RunLogLevel;
  stage: string;
  event: string;
  message: string;
  payload?: Record<string, unknown>;
}

export type OpenAITelemetryCallback = (event: OpenAITelemetryEvent) => void;

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

function serializeError(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    const maybeError = error as Error & {
      status?: number;
      code?: string;
      type?: string;
    };
    return {
      name: maybeError.name,
      message: maybeError.message,
      status: maybeError.status ?? null,
      code: maybeError.code ?? null,
      type: maybeError.type ?? null,
    };
  }

  if (typeof error === "object" && error !== null) {
    return JSON.parse(JSON.stringify(error)) as Record<string, unknown>;
  }

  return {
    message: String(error),
  };
}

function sanitizeEmbeddingInput(text: string): string {
  const sanitized = text
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (sanitized.length === 0) {
    return "sem_descricao";
  }

  if (sanitized.length <= MAX_EMBEDDING_INPUT_CHARS) {
    return sanitized;
  }

  return sanitized.slice(0, MAX_EMBEDDING_INPUT_CHARS);
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
  private readonly telemetry?: OpenAITelemetryCallback;

  private readonly stats: OpenAIProviderStats = {
    embedding_call_count: 0,
    category_profile_call_count: 0,
    attribute_batch_call_count: 0,
    category_disambiguation_call_count: 0,
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
    this.telemetry = options.telemetry;
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

  private emitTelemetry(event: OpenAITelemetryEvent): void {
    this.telemetry?.(event);
  }

  private async withRetry<T>(input: {
    callKind: "embedding" | "category_profile" | "attribute_batch" | "category_disambiguation";
    requestBody: Record<string, unknown>;
    operation: () => Promise<T>;
    responsePayloadFactory: (response: T) => Record<string, unknown>;
  }): Promise<T> {
    const totalAttempts = this.maxRetries + 1;

    this.emitTelemetry({
      level: "debug",
      stage: "openai",
      event: "openai.call.started",
      message: `OpenAI call started (${input.callKind}).`,
      payload: {
        call_kind: input.callKind,
        total_attempts: totalAttempts,
        request_body: input.requestBody,
      },
    });

    for (let attempt = 1; attempt <= totalAttempts; attempt += 1) {
      const attemptStartedAt = Date.now();

      this.emitTelemetry({
        level: "debug",
        stage: "openai",
        event: "openai.attempt.started",
        message: `OpenAI attempt started (${input.callKind}).`,
        payload: {
          call_kind: input.callKind,
          attempt,
          total_attempts: totalAttempts,
        },
      });

      try {
        const response = await this.withTimeout(input.operation());

        this.emitTelemetry({
          level: "debug",
          stage: "openai",
          event: "openai.attempt.succeeded",
          message: `OpenAI attempt succeeded (${input.callKind}).`,
          payload: {
            call_kind: input.callKind,
            attempt,
            elapsed_ms: Date.now() - attemptStartedAt,
            ...input.responsePayloadFactory(response),
          },
        });

        return response;
      } catch (error) {
        if (isTimeoutError(error)) {
          this.stats.timeout_count += 1;
        }

        const retryable = isRetryableError(error);
        const errorPayload = serializeError(error);

        this.emitTelemetry({
          level: "warn",
          stage: "openai",
          event: "openai.attempt.failed",
          message: `OpenAI attempt failed (${input.callKind}).`,
          payload: {
            call_kind: input.callKind,
            attempt,
            total_attempts: totalAttempts,
            retryable,
            elapsed_ms: Date.now() - attemptStartedAt,
            error: errorPayload,
          },
        });

        if (!retryable || attempt === totalAttempts) {
          this.stats.request_failure_count += 1;

          this.emitTelemetry({
            level: "error",
            stage: "openai",
            event: "openai.call.failed",
            message: `OpenAI call failed (${input.callKind}).`,
            payload: {
              call_kind: input.callKind,
              attempt,
              total_attempts: totalAttempts,
              error: errorPayload,
            },
          });

          throw error;
        }

        this.stats.retry_count += 1;
        const baseDelay = Math.min(this.retryMaxMs, this.retryBaseMs * 2 ** (attempt - 1));
        const jitteredDelay = Math.round(baseDelay * (0.8 + Math.random() * 0.4));

        this.emitTelemetry({
          level: "warn",
          stage: "openai",
          event: "openai.retry.scheduled",
          message: `OpenAI retry scheduled (${input.callKind}).`,
          payload: {
            call_kind: input.callKind,
            attempt,
            total_attempts: totalAttempts,
            delay_ms: jitteredDelay,
          },
        });

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

    const sanitizedTexts = texts.map((text) => sanitizeEmbeddingInput(text));

    this.stats.embedding_call_count += 1;
    const requestBody = {
      model: this.embeddingModel,
      input: sanitizedTexts,
      dimensions: this.dimensions,
    };

    const response = await this.withRetry({
      callKind: "embedding",
      requestBody,
      operation: () => this.client.embeddings.create(requestBody),
      responsePayloadFactory: (embeddingResponse) => ({
        response_metadata: {
          model: embeddingResponse.model,
          item_count: embeddingResponse.data.length,
          dimensions: embeddingResponse.data[0]?.embedding?.length ?? this.dimensions,
          usage: embeddingResponse.usage ?? null,
        },
      }),
    });

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
    const requestBody = {
      model: this.llmModel,
      response_format: { type: "json_object" as const },
      messages: [
        {
          role: "system" as const,
          content:
            "Retorne JSON no formato: {name_pt, description_pt, synonyms, attributes:[{key,label_pt,type,allowed_values?,required}]}",
        },
        {
          role: "user" as const,
          content: prompt,
        },
      ],
      temperature: 0.2,
    };

    const completion = await this.withRetry({
      callKind: "category_profile",
      requestBody,
      operation: () => this.client.chat.completions.create(requestBody),
      responsePayloadFactory: (chatResponse) => ({
        response_body: chatResponse,
      }),
    });

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
    const requestBody = {
      model: this.llmModel,
      response_format: { type: "json_object" as const },
      messages: [
        {
          role: "system" as const,
          content:
            "Retorne JSON no formato {results:[{source_sku, values:{[key]:valueOuNull}, confidence:{[key]:numero0a1}}]}. Use apenas chaves existentes no schema.",
        },
        {
          role: "user" as const,
          content: prompt,
        },
      ],
      temperature: 0,
    };

    const completion = await this.withRetry({
      callKind: "attribute_batch",
      requestBody,
      operation: () => this.client.chat.completions.create(requestBody),
      responsePayloadFactory: (chatResponse) => ({
        response_body: chatResponse,
      }),
    });

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

  async disambiguateCategory(
    input: CategoryDisambiguationInput,
  ): Promise<CategoryDisambiguationOutput> {
    if (input.candidates.length === 0) {
      return {
        categorySlug: null,
        confidence: 0,
        reason: "no_candidates",
      };
    }

    const prompt = [
      "Escolha a melhor categoria para o produto.",
      "Responda APENAS com JSON valido.",
      "Se estiver incerto, escolha null com baixa confianca.",
      `product: ${JSON.stringify(input.product)}`,
      `candidates: ${JSON.stringify(input.candidates)}`,
    ].join("\n");

    this.stats.category_disambiguation_call_count += 1;
    const requestBody = {
      model: this.llmModel,
      response_format: { type: "json_object" as const },
      messages: [
        {
          role: "system" as const,
          content:
            "Retorne JSON no formato {category_slug:string|null, confidence:number, reason:string}. category_slug deve ser um dos candidatos ou null.",
        },
        {
          role: "user" as const,
          content: prompt,
        },
      ],
      temperature: 0,
    };

    const completion = await this.withRetry({
      callKind: "category_disambiguation",
      requestBody,
      operation: () => this.client.chat.completions.create(requestBody),
      responsePayloadFactory: (chatResponse) => ({
        response_body: chatResponse,
      }),
    });

    const content = completion.choices[0]?.message?.content ?? "{}";
    const parsed = disambiguationSchema.parse(JSON.parse(content));
    const allowedSlugs = new Set(input.candidates.map((candidate) => candidate.slug));
    const chosenSlug =
      parsed.category_slug && allowedSlugs.has(parsed.category_slug) ? parsed.category_slug : null;

    return {
      categorySlug: chosenSlug,
      confidence: clampConfidence(parsed.confidence),
      reason: parsed.reason,
    };
  }
}

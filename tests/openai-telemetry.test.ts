import { describe, expect, it } from "vitest";
import { OpenAIProvider, type OpenAITelemetryEvent } from "../src/services/openai.js";

function createProvider(events: OpenAITelemetryEvent[]): OpenAIProvider {
  return new OpenAIProvider({
    apiKey: "test",
    llmModel: "gpt-4.1-mini",
    embeddingModel: "text-embedding-3-large",
    dimensions: 3,
    timeoutMs: 10_000,
    maxRetries: 1,
    retryBaseMs: 1,
    retryMaxMs: 2,
    telemetry: (event) => events.push(event),
  });
}

describe("openai telemetry", () => {
  it("captures summarized chat request/response payloads for category profile", async () => {
    const events: OpenAITelemetryEvent[] = [];
    const provider = createProvider(events);

    (provider as unknown as { client: unknown }).client = {
      chat: {
        completions: {
          create: async () => ({
            choices: [
              {
                message: {
                  content: JSON.stringify({
                    name_pt: "caderno a4",
                    description_pt: "Categoria de cadernos A4 para escola.",
                    synonyms: ["caderno escolar"],
                    attributes: [],
                  }),
                },
              },
            ],
          }),
        },
      },
      embeddings: {
        create: async () => ({ data: [] }),
      },
    };

    const output = await provider.generateCategoryProfile({
      candidateName: "caderno",
      sampleProducts: [{ title: "Caderno A4 pautado", description: "96 folhas", brand: "Note" }],
    });

    expect(output.name_pt).toBe("caderno a4");

    const started = events.find((event) => event.event === "openai.call.started");
    const succeeded = events.find((event) => event.event === "openai.attempt.succeeded");

    expect(started?.payload?.call_kind).toBe("category_profile");
    expect(started?.payload?.request_body).toBeTruthy();
    expect(succeeded?.payload?.response_metadata).toBeTruthy();
    expect(succeeded?.payload?.response_body).toBeUndefined();
  });

  it("emits retry telemetry events when first attempt fails", async () => {
    const events: OpenAITelemetryEvent[] = [];
    const provider = createProvider(events);

    let attempts = 0;
    (provider as unknown as { client: unknown }).client = {
      chat: {
        completions: {
          create: async () => {
            attempts += 1;
            if (attempts === 1) {
              throw { status: 429, message: "rate limit" };
            }

            return {
              choices: [
                {
                  message: {
                    content: JSON.stringify({
                      results: [
                        {
                          source_sku: "sku-1",
                          values: { format: "A4" },
                          confidence: { format: 0.9 },
                        },
                      ],
                    }),
                  },
                },
              ],
            };
          },
        },
      },
      embeddings: {
        create: async () => ({ data: [] }),
      },
    };

    const output = await provider.extractProductAttributesBatch({
      categoryName: "caderno a4",
      categoryDescription: "cadernos escolares",
      attributeSchema: {
        schema_version: "1.0",
        category_name_pt: "caderno a4",
        attributes: [
          {
            key: "format",
            label_pt: "formato",
            type: "enum",
            allowed_values: ["A4", "A5"],
            required: false,
          },
        ],
      },
      products: [
        {
          sourceSku: "sku-1",
          product: {
            title: "Caderno A4",
            description: "Pautado",
            brand: "Note",
          },
        },
      ],
    });

    expect(output["sku-1"]).toBeTruthy();
    expect(events.some((event) => event.event === "openai.retry.scheduled")).toBe(true);
  });

  it("does not log raw embedding vectors in telemetry response payload", async () => {
    const events: OpenAITelemetryEvent[] = [];
    const provider = createProvider(events);

    (provider as unknown as { client: unknown }).client = {
      chat: {
        completions: {
          create: async () => ({ choices: [] }),
        },
      },
      embeddings: {
        create: async () => ({
          model: "text-embedding-3-large",
          usage: { prompt_tokens: 10, total_tokens: 10 },
          data: [
            { index: 1, embedding: [0.1, 0.2, 0.3] },
            { index: 0, embedding: [0.4, 0.5, 0.6] },
          ],
        }),
      },
    };

    const vectors = await provider.embedMany(["abc", "def"]);
    expect(vectors).toHaveLength(2);

    const embeddingSuccess = events.find(
      (event) =>
        event.event === "openai.attempt.succeeded" && event.payload?.call_kind === "embedding",
    );

    expect(embeddingSuccess).toBeTruthy();
    expect(embeddingSuccess?.payload?.response_metadata).toBeTruthy();
    expect(embeddingSuccess?.payload?.response_body).toBeUndefined();
  });

  it("uses per-call model override for attribute extraction batches", async () => {
    const events: OpenAITelemetryEvent[] = [];
    const provider = createProvider(events);
    let receivedModel = "";

    (provider as unknown as { client: unknown }).client = {
      chat: {
        completions: {
          create: async (input: { model: string }) => {
            receivedModel = input.model;
            return {
              choices: [
                {
                  message: {
                    content: JSON.stringify({
                      results: [
                        {
                          source_sku: "sku-1",
                          values: { format: "A4" },
                          confidence: { format: 0.92 },
                        },
                      ],
                    }),
                  },
                },
              ],
            };
          },
        },
      },
      embeddings: {
        create: async () => ({ data: [] }),
      },
    };

    const output = await provider.extractProductAttributesBatch({
      categoryName: "caderno a4",
      categoryDescription: "cadernos escolares",
      model: "gpt-4.1",
      attributeSchema: {
        schema_version: "1.0",
        category_name_pt: "caderno a4",
        attributes: [
          {
            key: "format",
            label_pt: "formato",
            type: "enum",
            allowed_values: ["A4", "A5"],
            required: false,
          },
        ],
      },
      products: [
        {
          sourceSku: "sku-1",
          product: {
            title: "Caderno A4",
            description: "Pautado",
            brand: "Note",
          },
        },
      ],
    });

    expect(receivedModel).toBe("gpt-4.1");
    expect(output["sku-1"]?.values?.format).toBe("A4");

    const started = events.find(
      (event) =>
        event.event === "openai.call.started" &&
        event.payload?.call_kind === "attribute_batch",
    );
    const requestBody = started?.payload?.request_body as Record<string, unknown> | undefined;
    expect(requestBody?.model).toBe("gpt-4.1");
  });
});

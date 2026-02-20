import OpenAI from "openai";
import { z } from "zod";
import type {
  AttributeExtractionLLMOutput,
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

interface OpenAIProviderOptions {
  apiKey: string;
  llmModel: string;
  embeddingModel: string;
  dimensions: number;
}

export class OpenAIProvider implements EmbeddingProvider, LLMProvider {
  public readonly dimensions: number;

  private readonly client: OpenAI;
  private readonly llmModel: string;
  private readonly embeddingModel: string;

  constructor(options: OpenAIProviderOptions) {
    this.client = new OpenAI({ apiKey: options.apiKey });
    this.llmModel = options.llmModel;
    this.embeddingModel = options.embeddingModel;
    this.dimensions = options.dimensions;
  }

  async embedMany(texts: string[]): Promise<number[][]> {
    if (texts.length === 0) {
      return [];
    }

    const response = await this.client.embeddings.create({
      model: this.embeddingModel,
      input: texts,
      dimensions: this.dimensions,
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

    const completion = await this.client.chat.completions.create({
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
    const prompt = [
      "Extraia atributos do produto com base no schema da categoria.",
      "Responda APENAS com JSON valido.",
      "Nao invente valores; se nao encontrar, use null com baixa confianca.",
      `category_name: ${input.categoryName}`,
      `category_description: ${input.categoryDescription}`,
      `schema: ${JSON.stringify(input.attributeSchema)}`,
      `product: ${JSON.stringify(input.product)}`,
    ].join("\n");

    const completion = await this.client.chat.completions.create({
      model: this.llmModel,
      response_format: { type: "json_object" },
      messages: [
        {
          role: "system",
          content:
            "Retorne JSON no formato {values: {[key]: valueOuNull}, confidence: {[key]: numero0a1}}. Use apenas chaves existentes no schema.",
        },
        {
          role: "user",
          content: prompt,
        },
      ],
      temperature: 0,
    });

    const content = completion.choices[0]?.message?.content ?? "{}";
    const parsed = JSON.parse(content);
    return extractionSchema.parse(parsed);
  }
}

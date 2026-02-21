import type {
  AttributeExtractionLLMOutput,
  CategoryAttribute,
  CategoryAttributeSchema,
  LLMProvider,
  NormalizedCatalogProduct,
  ProductEnrichment,
  TaxonomyAttributePolicy,
} from "../types.js";
import {
  detectFormat,
  detectPackCount,
  detectRuling,
  normalizeText,
} from "../utils/text.js";
import { loadTaxonomy } from "../taxonomy/load.js";

const PACK_CONTEXT_REGEX = /(pack|caixa|conjunto|kit|unid|unidades|pcs|pecas|x\s*\d+)/;
const SHEET_CONTEXT_REGEX = /(folhas|fls|resma|caderno|bloco|recarga)/;
const BLOCKING_CATEGORY_REASONS = new Set([
  "below_auto_confidence",
  "low_margin",
  "generic_or_fallback_category",
  "category_contradiction",
  "out_of_scope_category",
  "missing_variant_for_auto",
]);

const taxonomy = loadTaxonomy();

function parseNumber(text: string, regex: RegExp): number | null {
  const match = normalizeText(text).match(regex);
  if (!match) {
    return null;
  }

  const value = Number(match[1].replace(",", "."));
  return Number.isFinite(value) ? value : null;
}

function detectBoolean(text: string, positive: RegExp, negative: RegExp): boolean | null {
  const normalized = normalizeText(text);
  if (negative.test(normalized)) {
    return false;
  }
  if (positive.test(normalized)) {
    return true;
  }
  return null;
}

function detectPackCountStrict(text: string): number | null {
  const normalized = normalizeText(text);

  const packMatch = normalized.match(/(?:pack|caixa|conjunto|kit)\s*(?:de\s*)?(\d{1,3})\b/);
  if (packMatch) {
    return Number(packMatch[1]);
  }

  const bundledUnitsMatch = normalized.match(
    /\b(\d{1,3})\s*x\s*\d{1,4}\s*(?:un|unid|unidades|uds|pcs|pecas)\b/,
  );
  if (bundledUnitsMatch) {
    return Number(bundledUnitsMatch[1]);
  }

  const unitsMatch = normalized.match(/\b(\d{1,3})\s*(?:unid(?:ades)?|un|uds|pcs|pecas)\b/);
  if (unitsMatch) {
    return Number(unitsMatch[1]);
  }

  return null;
}

function detectCompartmentCount(text: string): number | null {
  const normalized = normalizeText(text);
  const numericMatch = normalized.match(/\b(\d{1,2})\s*(?:compartimentos?|fechos?|ziperes?|zipper)\b/);
  if (numericMatch) {
    return Number(numericMatch[1]);
  }
  if (/\btriplo\b/.test(normalized)) {
    return 3;
  }
  if (/\bduplo\b/.test(normalized)) {
    return 2;
  }
  if (/\bsimples\b/.test(normalized)) {
    return 1;
  }
  return null;
}

function detectTipType(text: string): string | null {
  const normalized = normalizeText(text);
  if (/(chanfrad|chisel|marca texto|highlighter)/.test(normalized)) {
    return "chanfrada";
  }
  if (/(fina|fine|fineliner|0[.,][2-6]|0[.,]7)/.test(normalized)) {
    return "fina";
  }
  if (/(media|medium|brush|pincel|1[.,]0|1[.,]2)/.test(normalized)) {
    return "media";
  }
  return null;
}

function detectTargetAge(text: string): string | null {
  const normalized = normalizeText(text);
  if (/(infantil|crianca|criança|kids|junior|miudo|miúdo)/.test(normalized)) {
    return "infantil";
  }
  if (/(juvenil|teen|adolescente)/.test(normalized)) {
    return "juvenil";
  }
  return null;
}

function detectPaintType(text: string): string | null {
  const normalized = normalizeText(text);
  if (/guache/.test(normalized)) {
    return "guache";
  }
  if (/aquarela/.test(normalized)) {
    return "aquarela";
  }
  if (/(acrilica|acrílica)/.test(normalized)) {
    return "acrilica";
  }
  if (/(tempera|têmpera)/.test(normalized)) {
    return "tempera";
  }
  return null;
}

function detectBrushType(text: string): string | null {
  const normalized = normalizeText(text);
  if (/(pincel\s*chato|flat brush)/.test(normalized)) {
    return "chato";
  }
  if (/(pincel\s*redondo|round brush)/.test(normalized)) {
    return "redondo";
  }
  if (/esponja/.test(normalized)) {
    return "esponja";
  }
  return null;
}

function detectTapeType(text: string): string | null {
  const normalized = normalizeText(text);
  if (/(dupla\s*face|double\s*face)/.test(normalized)) {
    return "dupla_face";
  }
  if (/(masking|papel crepe|fita de papel)/.test(normalized)) {
    return "papel";
  }
  if (/(fita|adesiva|durex|rollafix|washi)/.test(normalized)) {
    return "transparente";
  }
  return null;
}

function detectColorSet(text: string): boolean | null {
  const normalized = normalizeText(text);
  if (/(\d+\s*cores|multicor|colorido|sortido)/.test(normalized)) {
    return true;
  }
  if (/(preto|azul|vermelho|unica cor|única cor)/.test(normalized)) {
    return false;
  }
  return null;
}

function detectItemSubtype(categorySlug: string, text: string, titleText: string): string | null {
  const normalized = normalizeText(text);
  const normalizedTitle = normalizeText(titleText);
  const inTitle = (pattern: RegExp): boolean => pattern.test(normalizedTitle);
  const inText = (pattern: RegExp): boolean => pattern.test(normalized);

  if (categorySlug === "cadernos_blocos") {
    if (inTitle(/(flash\s*cards?|fichas?)/) || inText(/(flash\s*cards?|fichas?)/)) {
      return "flashcards";
    }
    if (inTitle(/(bloco.*desenho|desenho|croquis|sketch|cartolina|papel vegetal)/) || inText(/(bloco.*desenho|desenho|croquis|sketch|cartolina|papel vegetal)/)) {
      return "bloco_desenho";
    }
    if (inTitle(/recarga/) || inText(/recarga/)) {
      return "recarga";
    }
    if (inTitle(/(bloco|apontamentos|bloco de notas)/) || inText(/(bloco|apontamentos|bloco de notas)/)) {
      return "bloco_apontamentos";
    }
    if (inTitle(/(caderno|espiral|brochura|caderno inteligente)/) || inText(/(caderno|espiral|brochura|caderno inteligente)/)) {
      return "caderno";
    }
    return null;
  }

  if (categorySlug === "escrita") {
    if (inTitle(/(afiador|apontador|afia)/) || (!inTitle(/(lapis|lápis|lapiseira)/) && inText(/(afiador|apontador|afia)/))) {
      return "afiador";
    }
    if (inTitle(/(borracha|eraser)/) || (!inTitle(/(lapis|lápis|lapiseira)/) && inText(/(borracha|eraser)/))) {
      return "borracha";
    }
    if (inTitle(/(corretor|corretivo|corrector|correctora)/) || inText(/(corretor|corretivo|corrector|correctora)/)) {
      return "corretor";
    }
    if (inTitle(/(lapis de cor|lápis de cor|colored pencils)/) || inText(/(lapis de cor|lápis de cor|colored pencils)/)) {
      return "lapis_cor";
    }
    if (inTitle(/(caneta|esferografica|esferográfica|gel pen|roller)/) || inText(/(caneta|esferografica|esferográfica|gel pen|roller)/)) {
      return "caneta";
    }
    if (inTitle(/(marca texto|marcador|highlighter|fineliner)/) || inText(/(marca texto|marcador|highlighter|fineliner)/)) {
      return "marcador";
    }
    if (inTitle(/(lapis|lápis|lapiseira|grafite)/) || inText(/(lapis|lápis|lapiseira|grafite)/)) {
      return "lapis_grafite";
    }
    return null;
  }

  if (categorySlug === "organizacao_arquivo") {
    if (inTitle(/(capa com elastico|capa com elasticos|bolsa catalogo|bolsa catálogo|pasta|arquivo|classificador|dossier|separador|envelope)/) || inText(/(capa com elastico|capa com elasticos|bolsa catalogo|bolsa catálogo|pasta|arquivo|classificador|dossier|separador|envelope)/)) {
      return "pasta";
    }
    if (inTitle(/(etiquetas?|label|rotulo|rótulo)/) || inText(/(etiquetas?|label|rotulo|rótulo)/)) {
      return "etiqueta";
    }
    if (inTitle(/(agrafador|stapler|saca\s*agrafos)/) || inText(/(agrafador|stapler|saca\s*agrafos)/)) {
      return "agrafador";
    }
    if (inTitle(/\bagrafos\b/) || inText(/\bagrafos\b/)) {
      return "agrafos";
    }
    if (inTitle(/\bclips?\b/) || inText(/\bclips?\b/)) {
      return "clips";
    }
    if (inTitle(/(post-it|post it|notas aderentes|bloco adesivo)/) || inText(/(post-it|post it|notas aderentes|bloco adesivo)/)) {
      return "bloco_aderente";
    }
    return null;
  }

  if (categorySlug === "geometria_corte") {
    if (inTitle(/tesoura/) || inText(/tesoura/)) {
      return "tesoura";
    }
    if (inTitle(/compasso/) || inText(/compasso/)) {
      return "compasso";
    }
    if (inTitle(/(esquadro|transferidor)/) || inText(/(esquadro|transferidor)/)) {
      return "esquadro_transferidor";
    }
    if (inTitle(/(regua|régua)/) || inText(/(regua|régua)/)) {
      return "regua";
    }
    return null;
  }

  if (categorySlug === "transporte_escolar") {
    if (inTitle(/(estojo|penal|porta lapis|porta lápis|necessaire|nécessaire)/)) {
      return "estojo";
    }
    if (inTitle(/(mochila|backpack|trolley)/)) {
      return "mochila";
    }
    if (
      inText(/(estojo|penal|porta lapis|porta lápis|necessaire|nécessaire)/) &&
      !inTitle(/(borracha|afiador|apontador|afia|agrafador|agrafos|classificador|capa com elastico|capa com elasticos)/)
    ) {
      return "estojo";
    }
    if (inText(/(mochila|backpack|trolley)/)) {
      return "mochila";
    }
    return null;
  }

  if (categorySlug === "artes") {
    if (inTitle(/(tinta|guache|aquarela|acrilica|acrílica|tempera|têmpera)/) || inText(/(tinta|guache|aquarela|acrilica|acrílica|tempera|têmpera)/)) {
      return "tinta";
    }
    if (inTitle(/pincel/) || inText(/pincel/)) {
      return "pincel";
    }
    if (inTitle(/(papel artistico|papel artístico|cartolina|bloco desenho|croquis|sketch)/) || inText(/(papel artistico|papel artístico|cartolina|bloco desenho|croquis|sketch)/)) {
      return "papel_artistico";
    }
    if (inTitle(/(kit artistico|kit artístico|conjunto artistico|conjunto artístico)/) || inText(/(kit artistico|kit artístico|conjunto artistico|conjunto artístico)/)) {
      return "kit_artistico";
    }
    return null;
  }

  if (categorySlug === "cola_adesivos") {
    if (inTitle(/(corretor|corretivo|corrector|correctora|caneta corretora)/)) {
      return null;
    }
    if (inTitle(/(fita|adesiva|dupla face|rollafix|durex|washi|masking)/) || inText(/(fita|adesiva|dupla face|rollafix|durex|washi|masking)/)) {
      return "fita";
    }
    if (inTitle(/(cola|bastao|bastão|liquida|líquida|pva|stick)/) || inText(/(cola|bastao|bastão|liquida|líquida|pva|stick)/)) {
      return "cola";
    }
    return null;
  }

  if (categorySlug === "papel") {
    if (inTitle(/recarga/) || inText(/recarga/)) {
      return "recarga_papel";
    }
    if (inTitle(/(resma|papel a4|papel de impressao|papel de impressão|500 folhas|80g|70g)/) || inText(/(resma|papel a4|papel de impressao|papel de impressão|500 folhas|80g|70g)/)) {
      return "resma";
    }
    return null;
  }

  if (categorySlug === "outros_escolares") {
    if (/calculadora/.test(normalized)) {
      return "calculadora";
    }
    if (/(kit escolar|conjunto escolar)/.test(normalized)) {
      return "kit_escolar";
    }
    if (/(acessorio escolar|acessório escolar|material escolar)/.test(normalized)) {
      return "acessorio_escolar";
    }
    return null;
  }

  return null;
}

function isBlockingReason(reason: string): boolean {
  if (!reason) {
    return false;
  }

  if (BLOCKING_CATEGORY_REASONS.has(reason)) {
    return true;
  }

  return (
    reason === "empty_attribute_output" ||
    reason === "low_category_confidence" ||
    reason === "category_review_gate" ||
    reason.startsWith("missing_required_") ||
    reason.startsWith("low_attribute_confidence_") ||
    reason.startsWith("invalid_") ||
    reason.startsWith("policy_") ||
    reason.startsWith("contradiction_") ||
    reason.startsWith("pack_count_") ||
    reason.startsWith("llm_")
  );
}

function inferAttributeWithRules(
  attribute: CategoryAttribute,
  product: NormalizedCatalogProduct,
  categorySlug: string,
): { value: string | number | boolean | null; confidence: number } {
  const text = `${product.normalizedTitle} ${product.normalizedDescription}`;

  switch (attribute.key) {
    case "item_subtype": {
      const subtype = detectItemSubtype(categorySlug, text, product.normalizedTitle);
      return { value: subtype, confidence: subtype ? 0.9 : 0.1 };
    }
    case "format": {
      const format = detectFormat(text);
      return { value: format, confidence: format ? 0.92 : 0.1 };
    }
    case "ruling": {
      const ruling = detectRuling(text);
      return { value: ruling, confidence: ruling ? 0.9 : 0.1 };
    }
    case "pack_count": {
      const packCount = detectPackCountStrict(text) ?? detectPackCount(text);
      return { value: packCount, confidence: packCount ? 0.88 : 0.1 };
    }
    case "sheet_count": {
      const sheetCount = parseNumber(text, /(\d{2,4})\s*(?:folhas|fls)/);
      return { value: sheetCount, confidence: sheetCount ? 0.88 : 0.1 };
    }
    case "point_size_mm": {
      const pointSize = parseNumber(text, /(\d(?:[.,]\d)?)\s*mm/);
      return { value: pointSize, confidence: pointSize ? 0.86 : 0.1 };
    }
    case "hardness": {
      const hardness = text.match(/\b(HB|2B|B|H)\b/i)?.[1]?.toUpperCase() ?? null;
      return { value: hardness, confidence: hardness ? 0.9 : 0.1 };
    }
    case "compartment_count": {
      const compartments = detectCompartmentCount(text);
      return { value: compartments, confidence: compartments ? 0.88 : 0.1 };
    }
    case "tip_type": {
      const tipType = detectTipType(text);
      return { value: tipType, confidence: tipType ? 0.86 : 0.1 };
    }
    case "glue_type": {
      if (/(fita|adesiva|dupla face|rollafix|washi|masking)/.test(text)) {
        return { value: null, confidence: 0.1 };
      }
      if (/(bastao|bastão|stick|stic)/.test(text)) {
        return { value: "bastao", confidence: 0.9 };
      }
      if (/(liquida|líquida|liquid|branca|pva|cola escolar|super cola)/.test(text)) {
        return { value: "liquida", confidence: 0.9 };
      }
      return { value: null, confidence: 0.1 };
    }
    case "tape_type": {
      const tapeType = detectTapeType(text);
      return { value: tapeType, confidence: tapeType ? 0.86 : 0.1 };
    }
    case "volume_ml": {
      const volume = parseNumber(text, /(\d{1,4})\s*ml/);
      if (volume) {
        return { value: volume, confidence: 0.84 };
      }
      const grams = parseNumber(text, /(\d{1,4}(?:[.,]\d+)?)\s*g\b/);
      return { value: grams, confidence: grams ? 0.68 : 0.1 };
    }
    case "has_wheels": {
      const value = detectBoolean(text, /(com rodas|rodas|trolley)/, /(sem rodas)/);
      return { value, confidence: value !== null ? 0.86 : 0.1 };
    }
    case "capacity_l": {
      const capacity = parseNumber(text, /(\d{1,3}(?:[.,]\d+)?)\s*(?:l|litros?)/);
      return { value: capacity, confidence: capacity ? 0.84 : 0.1 };
    }
    case "target_age": {
      const targetAge = detectTargetAge(text);
      return { value: targetAge, confidence: targetAge ? 0.8 : 0.1 };
    }
    case "tip_safety": {
      const tipSafety = detectBoolean(text, /(ponta redonda|seguranca|segurança)/, /(ponta afiada)/);
      return { value: tipSafety, confidence: tipSafety !== null ? 0.82 : 0.1 };
    }
    case "length_cm": {
      const length = parseNumber(text, /(\d{1,3}(?:[.,]\d+)?)\s*cm/);
      return { value: length, confidence: length ? 0.83 : 0.1 };
    }
    case "ink_type": {
      if (/gel/.test(text)) {
        return { value: "gel", confidence: 0.9 };
      }
      if (/(esferografica|esferografico|esferográfica|esferográfico|ballpoint|bic|cristal)/.test(text)) {
        return { value: "esferografica", confidence: 0.88 };
      }
      if (/roller/.test(text)) {
        return { value: "roller", confidence: 0.88 };
      }
      if (/(marcador|marca texto|highlighter)/.test(text)) {
        return { value: "marcador", confidence: 0.82 };
      }
      return { value: null, confidence: 0.1 };
    }
    case "paint_type": {
      const paintType = detectPaintType(text);
      return { value: paintType, confidence: paintType ? 0.85 : 0.1 };
    }
    case "brush_type": {
      const brushType = detectBrushType(text);
      return { value: brushType, confidence: brushType ? 0.82 : 0.1 };
    }
    case "weight_gsm": {
      const gsm = parseNumber(text, /(\d{2,3})\s*(?:g\/m2|g\/m²|g)\b/);
      return { value: gsm, confidence: gsm ? 0.82 : 0.1 };
    }
    case "color_set": {
      const colorSet = detectColorSet(text);
      return { value: colorSet, confidence: colorSet !== null ? 0.82 : 0.1 };
    }
    default:
      return { value: null, confidence: 0.05 };
  }
}

function normalizeEnumValue(attribute: CategoryAttribute, rawValue: unknown): string | null {
  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return null;
  }

  const input = normalizeText(String(rawValue));
  if (!attribute.allowed_values || attribute.allowed_values.length === 0) {
    return input || null;
  }

  const matched = attribute.allowed_values.find((allowed) => normalizeText(allowed) === input);
  if (matched) {
    return matched;
  }

  const partial = attribute.allowed_values.find((allowed) => {
    const normalizedAllowed = normalizeText(allowed);
    return normalizedAllowed.includes(input) || input.includes(normalizedAllowed);
  });

  return partial ?? null;
}

function coerceValue(attribute: CategoryAttribute, value: unknown): string | number | boolean | null {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  if (attribute.type === "enum") {
    return normalizeEnumValue(attribute, value);
  }

  if (attribute.type === "number") {
    const parsed = Number(String(value).replace(",", "."));
    return Number.isFinite(parsed) ? parsed : null;
  }

  if (attribute.type === "boolean") {
    if (typeof value === "boolean") {
      return value;
    }
    const normalized = normalizeText(String(value));
    if (["sim", "true", "1", "yes", "com"].includes(normalized)) {
      return true;
    }
    if (["nao", "não", "false", "0", "no", "sem"].includes(normalized)) {
      return false;
    }
    return null;
  }

  return String(value).trim() || null;
}

function sanitizeConfidence(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Math.max(0, Math.min(1, parsed));
}

function hasAnyAttributeValue(values: Record<string, string | number | boolean | null>): boolean {
  return Object.values(values).some((value) => value !== null && value !== "");
}

function getAttributePolicy(categorySlug: string, attributeKey: string): TaxonomyAttributePolicy | null {
  const categoryPolicies = taxonomy.attributePolicies.category_attribute_overrides?.[categorySlug] ?? {};
  if (categoryPolicies[attributeKey]) {
    return categoryPolicies[attributeKey];
  }
  return taxonomy.attributePolicies.attribute_policies?.[attributeKey] ?? null;
}

function applyNumberPolicy(
  categorySlug: string,
  attribute: CategoryAttribute,
  value: number,
  productText: string,
): { value: number | null; reasons: string[]; remapSheetCount: number | null } {
  const policy = getAttributePolicy(categorySlug, attribute.key);
  const reasons: string[] = [];
  let remapSheetCount: number | null = null;

  if (policy) {
    if (policy.allow_negative === false && value < 0) {
      reasons.push(`policy_negative_${attribute.key}`);
      return { value: null, reasons, remapSheetCount };
    }

    if (typeof policy.min === "number" && value < policy.min) {
      reasons.push(`policy_min_${attribute.key}`);
      return { value: null, reasons, remapSheetCount };
    }

    if (typeof policy.max === "number" && value > policy.max) {
      reasons.push(`policy_max_${attribute.key}`);
      return { value: null, reasons, remapSheetCount };
    }

    if (policy.pack_context_required && attribute.key === "pack_count" && !PACK_CONTEXT_REGEX.test(productText)) {
      if (SHEET_CONTEXT_REGEX.test(productText) && value >= 20) {
        remapSheetCount = value;
        reasons.push("pack_count_remapped_to_sheet_count");
      } else {
        reasons.push("policy_pack_context_missing");
      }
      return { value: null, reasons, remapSheetCount };
    }
  }

  if (
    attribute.key === "pack_count" &&
    value > 80 &&
    SHEET_CONTEXT_REGEX.test(productText) &&
    !PACK_CONTEXT_REGEX.test(productText)
  ) {
    remapSheetCount = value;
    reasons.push("pack_count_suspect_sheet_count");
    return { value: null, reasons, remapSheetCount };
  }

  return { value, reasons, remapSheetCount };
}

function applyFamilyCrossChecks(
  categorySlug: string,
  attributeValues: Record<string, string | number | boolean | null>,
  reasons: string[],
): void {
  const subtype = String(attributeValues.item_subtype ?? "");

  if (categorySlug === "escrita") {
    if (subtype === "caneta" && attributeValues.hardness !== null && attributeValues.hardness !== "") {
      attributeValues.hardness = null;
      reasons.push("contradiction_hardness_for_caneta");
    }

    if (
      (subtype === "lapis_grafite" || subtype === "lapis_cor") &&
      attributeValues.ink_type !== null &&
      attributeValues.ink_type !== ""
    ) {
      attributeValues.ink_type = null;
      reasons.push("contradiction_ink_type_for_lapis");
    }
  }

  if (categorySlug === "cola_adesivos") {
    if (subtype === "fita" && attributeValues.glue_type !== null && attributeValues.glue_type !== "") {
      attributeValues.glue_type = null;
      reasons.push("contradiction_glue_type_for_fita");
    }

    if (subtype === "cola" && attributeValues.tape_type !== null && attributeValues.tape_type !== "") {
      attributeValues.tape_type = null;
      reasons.push("contradiction_tape_type_for_cola");
    }

    if (
      String(attributeValues.glue_type ?? "") === "bastao" &&
      String(attributeValues.item_subtype ?? "") === "fita"
    ) {
      attributeValues.glue_type = null;
      reasons.push("contradiction_glue_type_vs_subtype");
    }
  }
}

function shouldRequireSubtypeForAuto(categorySlug: string): boolean {
  return categorySlug !== "fora_escopo_escolar";
}

export async function enrichProduct(
  product: NormalizedCatalogProduct,
  category: {
    slug: string;
    attributes: CategoryAttributeSchema;
    description: string;
    confidenceScore: number;
    top2Confidence?: number;
    margin?: number;
    autoDecision?: "auto" | "review";
    confidenceReasons?: string[];
    isFallbackCategory?: boolean;
    contradictionCount?: number;
  },
  llm: LLMProvider | null,
  confidenceThreshold: number,
  attributeAutoMinConfidence = 0.7,
): Promise<ProductEnrichment> {
  let llmOutput: AttributeExtractionLLMOutput | null = null;
  let fallbackReason: string | undefined;

  if (llm) {
    try {
      llmOutput = await llm.extractProductAttributes({
        product: {
          title: product.title,
          description: product.description,
          brand: product.brand,
        },
        categoryName: category.attributes.category_name_pt,
        categoryDescription: category.description,
        attributeSchema: category.attributes,
      });
    } catch {
      llmOutput = null;
      fallbackReason = "llm_single_fallback";
    }
  }

  return enrichProductWithSignals(
    product,
    category,
    llmOutput,
    confidenceThreshold,
    attributeAutoMinConfidence,
    fallbackReason ? { fallbackReason } : undefined,
  );
}

export function enrichProductWithSignals(
  product: NormalizedCatalogProduct,
  category: {
    slug: string;
    attributes: CategoryAttributeSchema;
    description: string;
    confidenceScore: number;
    top2Confidence?: number;
    margin?: number;
    autoDecision?: "auto" | "review";
    confidenceReasons?: string[];
    isFallbackCategory?: boolean;
    contradictionCount?: number;
  },
  llmOutput: AttributeExtractionLLMOutput | null,
  confidenceThreshold: number,
  attributeAutoMinConfidence = 0.7,
  options?: {
    fallbackReason?: string;
  },
): ProductEnrichment {
  const productText = `${product.normalizedTitle} ${product.normalizedDescription}`;

  const ruleValues: Record<string, string | number | boolean | null> = {};
  const ruleConfidence: Record<string, number> = {};

  for (const attribute of category.attributes.attributes) {
    const inferred = inferAttributeWithRules(attribute, product, category.slug);
    ruleValues[attribute.key] = coerceValue(attribute, inferred.value);
    ruleConfidence[attribute.key] = sanitizeConfidence(inferred.confidence);
  }

  const llmValues = llmOutput?.values ?? {};
  const llmConfidence = llmOutput?.confidence ?? {};

  const attributeValues: Record<string, string | number | boolean | null> = {};
  const attributeConfidence: Record<string, number> = {};
  const reasons: string[] = [];

  for (const attribute of category.attributes.attributes) {
    const key = attribute.key;

    const ruleValue = coerceValue(attribute, ruleValues[key]);
    const ruleScore = sanitizeConfidence(ruleConfidence[key]);

    const llmValue = coerceValue(attribute, llmValues[key]);
    const llmScore = sanitizeConfidence(llmConfidence[key]);

    let chosenValue = ruleValue;
    let chosenConfidence = ruleScore;

    if ((chosenValue === null || chosenValue === "") && llmValue !== null && llmValue !== "") {
      chosenValue = llmValue;
      chosenConfidence = llmScore;
    }

    if (attribute.type === "enum" && chosenValue !== null && attribute.allowed_values && attribute.allowed_values.length > 0) {
      const normalized = normalizeText(String(chosenValue));
      const valid = attribute.allowed_values.some(
        (allowed) => normalizeText(allowed) === normalized,
      );
      if (!valid) {
        reasons.push(`invalid_enum_${key}`);
        chosenValue = null;
        chosenConfidence = Math.min(chosenConfidence, 0.2);
      }
    }

    if (typeof chosenValue === "number") {
      const policyValidation = applyNumberPolicy(category.slug, attribute, chosenValue, productText);
      chosenValue = policyValidation.value;
      for (const reason of policyValidation.reasons) {
        reasons.push(reason);
      }

      if (policyValidation.remapSheetCount !== null) {
        const hasSheetCountAttribute = category.attributes.attributes.some((item) => item.key === "sheet_count");
        if (hasSheetCountAttribute && (attributeValues.sheet_count === undefined || attributeValues.sheet_count === null)) {
          attributeValues.sheet_count = policyValidation.remapSheetCount;
          attributeConfidence.sheet_count = Math.max(0.7, chosenConfidence);
        }
      }
    }

    if (key === "ruling" && chosenValue !== null) {
      if (chosenValue === "quadriculado" && /pautado/.test(productText) && !/quadriculado/.test(productText)) {
        reasons.push("contradiction_ruling_text");
        chosenValue = null;
      }
      if (chosenValue === "pautado" && /quadriculado|milimetrado/.test(productText) && !/pautado/.test(productText)) {
        reasons.push("contradiction_ruling_text");
        chosenValue = null;
      }
    }

    if (chosenValue !== null && chosenConfidence < attributeAutoMinConfidence) {
      if (attribute.required) {
        reasons.push(`low_attribute_confidence_${key}`);
      } else {
        reasons.push(`low_optional_attribute_confidence_${key}`);
      }
    }

    attributeValues[key] = chosenValue;
    attributeConfidence[key] = chosenConfidence;

    if (attribute.required && (chosenValue === null || chosenValue === "")) {
      reasons.push(`missing_required_${key}`);
    }
  }

  applyFamilyCrossChecks(category.slug, attributeValues, reasons);

  if (
    category.autoDecision === "auto" &&
    shouldRequireSubtypeForAuto(category.slug) &&
    category.attributes.attributes.some((attribute) => attribute.key === "item_subtype") &&
    (attributeValues.item_subtype === null || attributeValues.item_subtype === undefined || attributeValues.item_subtype === "")
  ) {
    reasons.push("missing_variant_for_auto");
  }

  const hasRequiredAttributes = category.attributes.attributes.some((attribute) => attribute.required);
  if (
    !hasAnyAttributeValue(attributeValues) &&
    category.attributes.attributes.length > 0 &&
    hasRequiredAttributes
  ) {
    reasons.push("empty_attribute_output");
  }

  if (category.confidenceScore < confidenceThreshold && category.autoDecision !== "auto") {
    reasons.push("low_category_confidence");
  }

  if (category.autoDecision === "review") {
    reasons.push("category_review_gate");
  }

  if (category.confidenceReasons && category.confidenceReasons.length > 0) {
    for (const reason of category.confidenceReasons) {
      if (isBlockingReason(reason)) {
        reasons.push(reason);
      }
    }
  }

  if (options?.fallbackReason) {
    reasons.push(options.fallbackReason);
  }

  const uniqueReasons = [...new Set(reasons)].filter((reason) => isBlockingReason(reason));
  const attributeValidationFailCount = uniqueReasons.filter(
    (reason) =>
      reason.startsWith("invalid_") ||
      reason.startsWith("policy_") ||
      reason.startsWith("contradiction_") ||
      reason.startsWith("pack_count_"),
  ).length;

  const needsReview = category.autoDecision === "review" || uniqueReasons.length > 0;

  return {
    sourceSku: product.sourceSku,
    categorySlug: category.slug,
    categoryConfidence: category.confidenceScore,
    categoryTop2Confidence: category.top2Confidence ?? 0,
    categoryMargin: category.margin ?? Math.max(0, category.confidenceScore),
    autoDecision: category.autoDecision ?? (needsReview ? "review" : "auto"),
    confidenceReasons: category.confidenceReasons ?? [],
    isFallbackCategory: Boolean(category.isFallbackCategory),
    categoryContradictionCount: category.contradictionCount ?? 0,
    attributeValidationFailCount,
    attributeValues,
    attributeConfidence,
    needsReview,
    uncertaintyReasons: uniqueReasons,
  };
}

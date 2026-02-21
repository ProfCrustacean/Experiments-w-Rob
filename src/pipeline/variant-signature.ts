import { loadTaxonomy } from "../taxonomy/load.js";
import { normalizeText } from "../utils/text.js";

const VARIANT_PRIORITY_KEYS = [
  "item_subtype",
  "format",
  "ruling",
  "ink_type",
  "hardness",
  "glue_type",
  "tape_type",
  "paint_type",
  "brush_type",
  "point_size_mm",
  "sheet_count",
  "pack_count",
  "weight_gsm",
  "length_cm",
  "capacity_l",
  "compartment_count",
  "target_age",
] as const;

function readString(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  const text = String(value).trim();
  return text.length > 0 ? text : null;
}

function normalizedValue(value: unknown): string {
  return normalizeText(readString(value) ?? "");
}

export function buildVariantSignature(attributeValues: Record<string, unknown>): string {
  const parts: string[] = [];

  for (const key of VARIANT_PRIORITY_KEYS) {
    const raw = attributeValues[key];
    if (raw === null || raw === undefined || raw === "") {
      continue;
    }

    if (typeof raw === "boolean") {
      parts.push(`${key}=${raw ? "sim" : "nao"}`);
      continue;
    }

    parts.push(`${key}=${String(raw)}`);
  }

  return parts.join(" | ");
}

export function deriveLegacySplitHint(
  categorySlug: string,
  attributeValues: Record<string, unknown>,
): string {
  const taxonomy = loadTaxonomy();
  const subtype = normalizedValue(attributeValues.item_subtype);
  const format = normalizedValue(attributeValues.format);
  const ruling = normalizedValue(attributeValues.ruling);
  const inkType = normalizedValue(attributeValues.ink_type);
  const glueType = normalizedValue(attributeValues.glue_type);
  const tapeType = normalizedValue(attributeValues.tape_type);

  if (categorySlug === "cadernos_blocos") {
    if (subtype === "caderno") {
      if (format && ruling) {
        return `caderno-${format}-${ruling}`;
      }
      if (format) {
        return `caderno-${format}`;
      }
      return "caderno-a4-pautado";
    }
    if (subtype === "bloco_desenho") {
      return "bloco-desenho";
    }
    if (subtype === "bloco_apontamentos") {
      return "bloco-apontamentos";
    }
    if (subtype === "flashcards") {
      return "flashcards";
    }
    if (subtype === "recarga") {
      return "recarga-caderno";
    }
  }

  if (categorySlug === "escrita") {
    if (subtype === "caneta") {
      if (inkType === "gel") {
        return "caneta-gel";
      }
      return "caneta-esferografica";
    }
    if (subtype === "lapis_grafite") {
      return "lapis-hb";
    }
    if (subtype === "lapis_cor") {
      return "lapis-cor";
    }
    if (subtype === "marcador") {
      return "marcador-texto";
    }
    if (subtype === "borracha") {
      return "borracha";
    }
    if (subtype === "afiador") {
      return "afia";
    }
    if (subtype === "corretor") {
      return "corretor";
    }
  }

  if (categorySlug === "organizacao_arquivo") {
    if (subtype === "pasta") {
      return "pasta-arquivo";
    }
    if (subtype === "etiqueta") {
      return "etiquetas-adesivas";
    }
    if (subtype === "agrafador") {
      return "agrafador";
    }
    if (subtype === "agrafos") {
      return "agrafos-24-6";
    }
    if (subtype === "clips") {
      return "clips-papel";
    }
    if (subtype === "bloco_aderente") {
      return "bloco-notas-aderentes";
    }
  }

  if (categorySlug === "geometria_corte") {
    if (subtype === "regua") {
      return "regua";
    }
    if (subtype === "esquadro_transferidor") {
      return "esquadro-transferidor";
    }
    if (subtype === "tesoura") {
      return "tesoura";
    }
    if (subtype === "compasso") {
      return "compasso";
    }
  }

  if (categorySlug === "transporte_escolar") {
    if (subtype === "mochila") {
      return "mochila";
    }
    if (subtype === "estojo") {
      return "estojo";
    }
  }

  if (categorySlug === "artes") {
    if (subtype === "tinta") {
      return "tinta-artistica";
    }
    if (subtype === "pincel") {
      return "pincel-artistico";
    }
    if (subtype === "papel_artistico") {
      return "bloco-desenho";
    }
    if (subtype === "kit_artistico") {
      return "kit-artistico";
    }
  }

  if (categorySlug === "cola_adesivos") {
    if (subtype === "fita" || tapeType.length > 0) {
      return "fita-adesiva";
    }
    if (subtype === "cola") {
      if (glueType === "bastao") {
        return "cola-bastao";
      }
      return "cola-liquida";
    }
  }

  if (categorySlug === "papel") {
    if (format === "a4") {
      return "papel-a4";
    }
    if (subtype === "resma") {
      return "papel-a4";
    }
  }

  if (categorySlug === "outros_escolares") {
    if (subtype === "calculadora") {
      return "calculadora";
    }
    return "material-escolar-diverso";
  }

  if (categorySlug === "fora_escopo_escolar") {
    return "fora-escopo-escolar";
  }

  const fallbackLegacy = taxonomy.legacyByFamily.get(categorySlug);
  if (fallbackLegacy && fallbackLegacy.length > 0) {
    return fallbackLegacy[0];
  }

  return "";
}

import type { CategoryAttribute } from "../types.js";

export interface CategoryHintRule {
  key: string;
  labelPt: string;
  keywords: string[];
  synonyms: string[];
  defaultAttributes: CategoryAttribute[];
}

export const CATEGORY_HINT_RULES: CategoryHintRule[] = [
  {
    key: "caderno",
    labelPt: "Caderno",
    keywords: ["caderno", "notebook", "espiral", "brochura"],
    synonyms: ["caderno escolar", "bloco de notas"],
    defaultAttributes: [
      {
        key: "format",
        label_pt: "formato",
        type: "enum",
        allowed_values: ["A4", "A5"],
        required: false,
      },
      {
        key: "ruling",
        label_pt: "tipo_de_folha",
        type: "enum",
        allowed_values: ["pautado", "quadriculado", "liso"],
        required: false,
      },
      {
        key: "sheet_count",
        label_pt: "numero_de_folhas",
        type: "number",
        required: false,
      },
    ],
  },
  {
    key: "caneta",
    labelPt: "Caneta",
    keywords: ["caneta", "esferografica", "gel", "roller"],
    synonyms: ["caneta escolar", "esferografica"],
    defaultAttributes: [
      {
        key: "ink_type",
        label_pt: "tipo_de_tinta",
        type: "enum",
        allowed_values: ["gel", "esferografica", "roller"],
        required: false,
      },
      {
        key: "point_size_mm",
        label_pt: "espessura_ponta_mm",
        type: "number",
        required: false,
      },
      {
        key: "pack_count",
        label_pt: "quantidade_no_pack",
        type: "number",
        required: false,
      },
    ],
  },
  {
    key: "lapis",
    labelPt: "Lapis",
    keywords: ["lapis", "grafite", "hb", "2b", "cor"],
    synonyms: ["lapis escolar", "lapis grafite"],
    defaultAttributes: [
      {
        key: "hardness",
        label_pt: "graduacao",
        type: "enum",
        allowed_values: ["HB", "2B", "B", "H"],
        required: false,
      },
      {
        key: "pack_count",
        label_pt: "quantidade_no_pack",
        type: "number",
        required: false,
      },
      {
        key: "color_set",
        label_pt: "conjunto_de_cores",
        type: "boolean",
        required: false,
      },
    ],
  },
  {
    key: "borracha",
    labelPt: "Borracha",
    keywords: ["borracha", "apaga", "eraser"],
    synonyms: ["borracha escolar"],
    defaultAttributes: [
      {
        key: "material",
        label_pt: "material",
        type: "enum",
        allowed_values: ["vinil", "borracha", "plastica"],
        required: false,
      },
      {
        key: "pack_count",
        label_pt: "quantidade_no_pack",
        type: "number",
        required: false,
      },
    ],
  },
  {
    key: "cola",
    labelPt: "Cola",
    keywords: ["cola", "glue", "bastao", "liquida"],
    synonyms: ["cola escolar"],
    defaultAttributes: [
      {
        key: "glue_type",
        label_pt: "tipo_de_cola",
        type: "enum",
        allowed_values: ["bastao", "liquida"],
        required: false,
      },
      {
        key: "volume_ml",
        label_pt: "volume_ml",
        type: "number",
        required: false,
      },
    ],
  },
  {
    key: "mochila",
    labelPt: "Mochila",
    keywords: ["mochila", "backpack", "escolar", "rodas"],
    synonyms: ["mochila escolar"],
    defaultAttributes: [
      {
        key: "has_wheels",
        label_pt: "tem_rodas",
        type: "boolean",
        required: false,
      },
      {
        key: "capacity_l",
        label_pt: "capacidade_litros",
        type: "number",
        required: false,
      },
      {
        key: "target_age",
        label_pt: "faixa_etaria",
        type: "text",
        required: false,
      },
    ],
  },
  {
    key: "estojo",
    labelPt: "Estojo",
    keywords: ["estojo", "porta lapis", "penal"],
    synonyms: ["estojo escolar", "porta lapis"],
    defaultAttributes: [
      {
        key: "compartment_count",
        label_pt: "numero_de_compartimentos",
        type: "number",
        required: false,
      },
      {
        key: "material",
        label_pt: "material",
        type: "text",
        required: false,
      },
    ],
  },
  {
    key: "marcador",
    labelPt: "Marcador",
    keywords: ["marcador", "marca texto", "highlighter"],
    synonyms: ["marca texto", "marcador escolar"],
    defaultAttributes: [
      {
        key: "tip_type",
        label_pt: "tipo_de_ponta",
        type: "enum",
        allowed_values: ["chanfrada", "fina", "media"],
        required: false,
      },
      {
        key: "pack_count",
        label_pt: "quantidade_no_pack",
        type: "number",
        required: false,
      },
    ],
  },
  {
    key: "regua",
    labelPt: "Regua",
    keywords: ["regua", "esquadro", "transferidor"],
    synonyms: ["regua escolar"],
    defaultAttributes: [
      {
        key: "length_cm",
        label_pt: "comprimento_cm",
        type: "number",
        required: false,
      },
      {
        key: "material",
        label_pt: "material",
        type: "enum",
        allowed_values: ["plastico", "metal", "madeira"],
        required: false,
      },
    ],
  },
  {
    key: "tesoura",
    labelPt: "Tesoura",
    keywords: ["tesoura", "scissor"],
    synonyms: ["tesoura escolar"],
    defaultAttributes: [
      {
        key: "tip_safety",
        label_pt: "ponta_de_seguranca",
        type: "boolean",
        required: false,
      },
      {
        key: "length_cm",
        label_pt: "comprimento_cm",
        type: "number",
        required: false,
      },
    ],
  },
];

export const FALLBACK_CATEGORY_RULE: CategoryHintRule = {
  key: "material_escolar_diverso",
  labelPt: "Material Escolar Diverso",
  keywords: [],
  synonyms: ["material escolar"],
  defaultAttributes: [
    {
      key: "pack_count",
      label_pt: "quantidade_no_pack",
      type: "number",
      required: false,
    },
    {
      key: "color",
      label_pt: "cor",
      type: "text",
      required: false,
    },
  ],
};

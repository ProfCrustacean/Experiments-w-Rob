import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import type { PoolClient } from "pg";
import { getPool } from "../db/client.js";
import type {
  AppliedChangeRecord,
  CategoryDraft,
  HarnessEvalResult,
  NormalizedCatalogProduct,
  PipelineRunLogRow,
  PersistedCategory,
  PersistedProduct,
  ProductEnrichment,
  RunArtifactFormat,
  SelfImproveProposal,
  SelfImproveProposalKind,
  SelfImproveProposalPayload,
  SelfImproveProposalStatus,
  SelfCorrectionContext,
  SelfImproveAutoApplyPolicy,
  SelfImproveBatchStatus,
  SelfImproveLoopType,
  SelfImproveRunStatus,
} from "../types.js";
import { vectorToSqlLiteral } from "./embedding.js";
import { artifactKeyToFormat, type RunArtifactKey } from "./run-artifacts.js";
import {
  applyProposalPayloadToRules,
  dedupeTerms,
  readCategoryRulesFile,
  writeCategoryRulesFile,
  type CategoryRuleFile,
} from "./rule-patch.js";

export interface StoredRunArtifact {
  key: string;
  fileName: string;
  format: RunArtifactFormat;
  mimeType: string;
  sizeBytes: number;
  expiresAt: string;
  createdAt: string;
}

export interface StoredRunArtifactWithContent extends StoredRunArtifact {
  content: Buffer;
}

export interface PipelineRunListItem {
  runId: string;
  storeId: string;
  inputFileName: string;
  runLabel: string | null;
  status: string;
  startedAt: string;
  finishedAt: string | null;
  stats: Record<string, unknown>;
}

export interface SelfImprovementBatchSummary {
  total_loops?: number;
  completed_loops?: number;
  failed_loops?: number;
  running_sequence?: number | null;
  success_count?: number;
  retried_success_count?: number;
  final_failed_count?: number;
  gate_pass_rate?: number;
  auto_applied_updates_count?: number;
  [key: string]: unknown;
}

export interface SelfImprovementBatchListItem {
  id: string;
  requestedCount: number;
  loopType: SelfImproveLoopType;
  status: SelfImproveBatchStatus;
  maxLoopsCap: number;
  retryLimit: number;
  autoApplyPolicy: SelfImproveAutoApplyPolicy;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  summary: SelfImprovementBatchSummary;
}

export interface SelfImprovementRunItem {
  id: string;
  batchId: string;
  sequenceNo: number;
  attemptNo: number;
  status: SelfImproveRunStatus;
  pipelineRunId: string | null;
  error: Record<string, unknown>;
  selfCorrectionContext: Record<string, unknown>;
  gateResult: Record<string, unknown>;
  learningResult: Record<string, unknown>;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface SelfImprovementBatchDetails extends SelfImprovementBatchListItem {
  runs: SelfImprovementRunItem[];
}

interface PipelineRunQueryRow {
  id: string;
  store_id: string;
  input_file_name: string;
  run_label: string | null;
  status: string;
  started_at: Date | string;
  finished_at: Date | string | null;
  stats_json: Record<string, unknown>;
}

interface SelfImprovementBatchQueryRow {
  id: string;
  requested_count: number;
  loop_type: SelfImproveLoopType;
  status: SelfImproveBatchStatus;
  max_loops_cap: number;
  retry_limit: number;
  auto_apply_policy: SelfImproveAutoApplyPolicy;
  created_at: Date | string;
  started_at: Date | string | null;
  finished_at: Date | string | null;
  summary_jsonb: Record<string, unknown>;
}

interface SelfImprovementRunQueryRow {
  id: string;
  batch_id: string;
  sequence_no: number;
  attempt_no: number;
  status: SelfImproveRunStatus;
  pipeline_run_id: string | null;
  error_jsonb: Record<string, unknown>;
  self_correction_context_jsonb: Record<string, unknown>;
  gate_result_jsonb: Record<string, unknown>;
  learning_result_jsonb: Record<string, unknown>;
  started_at: Date | string | null;
  finished_at: Date | string | null;
}

interface SelfImproveProposalQueryRow {
  id: string;
  batch_id: string | null;
  run_id: string | null;
  proposal_kind: SelfImproveProposalKind;
  status: SelfImproveProposalStatus;
  confidence_score: number;
  expected_impact_score: number;
  payload_jsonb: Record<string, unknown>;
  source_jsonb: Record<string, unknown>;
  created_at: Date | string;
  updated_at: Date | string;
}

interface AppliedChangeQueryRow {
  id: string;
  proposal_id: string;
  proposal_kind: SelfImproveProposalKind;
  status: "applied" | "rolled_back";
  version_before: string;
  version_after: string;
  rollback_token: string;
  metadata_jsonb: Record<string, unknown>;
  applied_at: Date | string;
}

interface BenchmarkSnapshotQueryRow {
  id: string;
  store_id: string;
  source_jsonb: Record<string, unknown>;
  row_count: number;
  sample_size: number;
  dataset_hash: string;
  created_at: Date | string;
}

export interface BenchmarkSnapshot {
  id: string;
  storeId: string;
  source: Record<string, unknown>;
  rowCount: number;
  sampleSize: number;
  datasetHash: string;
  createdAt: string;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const CATEGORY_RULES_FILE_PATH = path.resolve(__dirname, "../taxonomy/category_match_rules.pt.json");

function toIsoString(value: Date | string): string {
  const date = value instanceof Date ? value : new Date(value);
  return date.toISOString();
}

function toEpochMs(value: Date | string): number {
  const date = value instanceof Date ? value : new Date(value);
  return date.getTime();
}

export async function createPipelineRun(input: {
  storeId: string;
  inputFileName: string;
  runLabel?: string;
}): Promise<string> {
  const pool = getPool();
  const result = await pool.query<{ id: string }>(
    `
      INSERT INTO pipeline_runs (store_id, input_file_name, run_label, status)
      VALUES ($1, $2, $3, 'running')
      RETURNING id
    `,
    [input.storeId, input.inputFileName, input.runLabel ?? null],
  );

  return result.rows[0].id;
}

export async function finalizePipelineRun(input: {
  runId: string;
  status: "completed_pending_review" | "accepted" | "rejected" | "failed";
  stats: Record<string, unknown>;
}): Promise<void> {
  const pool = getPool();
  await pool.query(
    `
      UPDATE pipeline_runs
      SET status = $2,
          finished_at = NOW(),
          stats_json = $3::jsonb
      WHERE id = $1
    `,
    [input.runId, input.status, JSON.stringify(input.stats)],
  );
}

export async function recoverStaleRunningRuns(input: {
  storeId: string;
  staleAfterMinutes: number;
}): Promise<number> {
  const pool = getPool();
  const result = await pool.query(
    `
      UPDATE pipeline_runs
      SET status = 'failed',
          finished_at = COALESCE(finished_at, NOW()),
          stats_json = COALESCE(stats_json, '{}'::jsonb)
            || jsonb_build_object(
              'error_message', 'stale_run_recovered_after_interrupt',
              'stale_recovered_at', NOW(),
              'stale_timeout_minutes', $2::int,
              'recovered_by', 'auto_stale_recovery'
            )
      WHERE store_id = $1
        AND status = 'running'
        AND started_at <= NOW() - (($2::text || ' minutes')::interval)
    `,
    [input.storeId, input.staleAfterMinutes],
  );

  return result.rowCount ?? 0;
}

export async function upsertRunArtifact(input: {
  runId: string;
  artifactKey: RunArtifactKey;
  fileName: string;
  mimeType: string;
  content: Buffer;
  expiresAt: Date;
}): Promise<void> {
  const pool = getPool();
  await pool.query(
    `
      INSERT INTO pipeline_run_artifacts (
        run_id,
        artifact_key,
        file_name,
        mime_type,
        content,
        size_bytes,
        expires_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (run_id, artifact_key)
      DO UPDATE SET
        file_name = EXCLUDED.file_name,
        mime_type = EXCLUDED.mime_type,
        content = EXCLUDED.content,
        size_bytes = EXCLUDED.size_bytes,
        expires_at = EXCLUDED.expires_at
    `,
    [
      input.runId,
      input.artifactKey,
      input.fileName,
      input.mimeType,
      input.content,
      input.content.byteLength,
      input.expiresAt.toISOString(),
    ],
  );
}

export async function listRunArtifacts(input: {
  runId: string;
  includeExpired?: boolean;
}): Promise<StoredRunArtifact[]> {
  const pool = getPool();
  const result = await pool.query<{
    artifact_key: string;
    file_name: string;
    mime_type: string;
    size_bytes: number;
    expires_at: Date | string;
    created_at: Date | string;
  }>(
    `
      SELECT
        artifact_key,
        file_name,
        mime_type,
        size_bytes,
        expires_at,
        created_at
      FROM pipeline_run_artifacts
      WHERE run_id = $1
        AND ($2::boolean = true OR expires_at > NOW())
      ORDER BY created_at DESC, artifact_key ASC
    `,
    [input.runId, Boolean(input.includeExpired)],
  );

  const artifacts: StoredRunArtifact[] = [];
  for (const row of result.rows) {
    const format = artifactKeyToFormat(row.artifact_key);
    if (!format) {
      continue;
    }

    artifacts.push({
      key: row.artifact_key,
      fileName: row.file_name,
      format,
      mimeType: row.mime_type,
      sizeBytes: row.size_bytes,
      expiresAt: toIsoString(row.expires_at),
      createdAt: toIsoString(row.created_at),
    });
  }

  return artifacts;
}

export async function getRunArtifact(input: {
  runId: string;
  artifactKey: RunArtifactKey;
}):
  Promise<{ status: "missing" } | { status: "expired"; expiresAt: string } | { status: "found"; artifact: StoredRunArtifactWithContent }> {
  const pool = getPool();
  const result = await pool.query<{
    artifact_key: string;
    file_name: string;
    mime_type: string;
    content: Buffer;
    size_bytes: number;
    expires_at: Date | string;
    created_at: Date | string;
  }>(
    `
      SELECT
        artifact_key,
        file_name,
        mime_type,
        content,
        size_bytes,
        expires_at,
        created_at
      FROM pipeline_run_artifacts
      WHERE run_id = $1
        AND artifact_key = $2
      LIMIT 1
    `,
    [input.runId, input.artifactKey],
  );

  if (result.rowCount === 0) {
    return { status: "missing" };
  }

  const row = result.rows[0];
  const format = artifactKeyToFormat(row.artifact_key);
  if (!format) {
    return { status: "missing" };
  }

  if (toEpochMs(row.expires_at) <= Date.now()) {
    return {
      status: "expired",
      expiresAt: toIsoString(row.expires_at),
    };
  }

  return {
    status: "found",
    artifact: {
      key: row.artifact_key,
      fileName: row.file_name,
      format,
      mimeType: row.mime_type,
      content: row.content,
      sizeBytes: row.size_bytes,
      expiresAt: toIsoString(row.expires_at),
      createdAt: toIsoString(row.created_at),
    },
  };
}

export async function cleanupExpiredRunArtifacts(retentionHours: number): Promise<number> {
  const pool = getPool();
  const result = await pool.query(
    `
      DELETE FROM pipeline_run_artifacts
      WHERE expires_at <= NOW()
         OR created_at <= NOW() - (($1::text || ' hours')::interval)
    `,
    [retentionHours],
  );
  return result.rowCount ?? 0;
}

export async function listPipelineRunsByStore(input: {
  storeId: string;
  limit: number;
}): Promise<PipelineRunListItem[]> {
  const pool = getPool();
  const result = await pool.query<PipelineRunQueryRow>(
    `
      SELECT
        id,
        store_id,
        input_file_name,
        run_label,
        status,
        started_at,
        finished_at,
        stats_json
      FROM pipeline_runs
      WHERE store_id = $1
      ORDER BY started_at DESC
      LIMIT $2
    `,
    [input.storeId, input.limit],
  );

  return result.rows.map(mapPipelineRunRow);
}

export async function getPipelineRunById(runId: string): Promise<PipelineRunListItem | null> {
  const pool = getPool();
  const result = await pool.query<PipelineRunQueryRow>(
    `
      SELECT
        id,
        store_id,
        input_file_name,
        run_label,
        status,
        started_at,
        finished_at,
        stats_json
      FROM pipeline_runs
      WHERE id = $1
      LIMIT 1
    `,
    [runId],
  );

  if (result.rowCount === 0) {
    return null;
  }

  return mapPipelineRunRow(result.rows[0]);
}


export {
  enqueueSelfImprovementBatch,
  listSelfImprovementBatches,
  getSelfImprovementBatchDetails,
  cancelSelfImprovementBatch,
  claimNextQueuedSelfImprovementBatch,
  recoverStaleSelfImprovementBatches,
  startSelfImprovementRunAttempt,
  finalizeSelfImprovementRunAttempt,
  finalizeSelfImprovementBatch,
  updateSelfImprovementBatchSummary,
  insertLearningProposals,
  listPendingLearningProposals,
  markProposalApplied,
  recordHarnessRun,
  createRollbackEvent,
  createBenchmarkSnapshot,
  getLatestBenchmarkSnapshot,
  getBenchmarkSnapshotById,
  applyTaxonomyAndRulePatchTransactional,
  recordAppliedChangeWithoutPatch,
  rollbackAppliedChangeTransactional,
  getAppliedChangeById,
  listRecentAppliedChanges,
  listRecentHarnessRuns,
} from "./persist-self-improvement.js";

function mapPipelineRunRow(row: PipelineRunQueryRow): PipelineRunListItem {
  return {
    runId: row.id,
    storeId: row.store_id,
    inputFileName: row.input_file_name,
    runLabel: row.run_label,
    status: row.status,
    startedAt: toIsoString(row.started_at),
    finishedAt: row.finished_at ? toIsoString(row.finished_at) : null,
    stats: row.stats_json ?? {},
  };
}

function mapRunLogRow(row: {
  run_id: string;
  seq: number;
  level: "debug" | "info" | "warn" | "error";
  stage: string;
  event: string;
  message: string;
  payload_jsonb: Record<string, unknown>;
  created_at: Date | string;
  expires_at: Date | string;
}): PipelineRunLogRow {
  return {
    runId: row.run_id,
    seq: row.seq,
    level: row.level,
    stage: row.stage,
    event: row.event,
    message: row.message,
    payload: row.payload_jsonb ?? {},
    timestamp: toIsoString(row.created_at),
    expiresAt: toIsoString(row.expires_at),
  };
}

export async function insertRunLogBatch(rows: PipelineRunLogRow[]): Promise<void> {
  if (rows.length === 0) {
    return;
  }

  const pool = getPool();
  const values: unknown[] = [];
  const placeholders: string[] = [];

  let position = 1;
  for (const row of rows) {
    values.push(
      row.runId,
      row.seq,
      row.level,
      row.stage,
      row.event,
      row.message,
      JSON.stringify(row.payload ?? {}),
      row.expiresAt,
    );
    placeholders.push(
      `($${position}, $${position + 1}, $${position + 2}, $${position + 3}, $${position + 4}, $${position + 5}, $${position + 6}::jsonb, $${position + 7})`,
    );
    position += 8;
  }

  await pool.query(
    `
      INSERT INTO pipeline_run_logs (
        run_id,
        seq,
        level,
        stage,
        event,
        message,
        payload_jsonb,
        expires_at
      )
      VALUES ${placeholders.join(", ")}
      ON CONFLICT (run_id, seq) DO NOTHING
    `,
    values,
  );
}

export async function listRunLogs(input: {
  runId: string;
  limit: number;
  stage?: string;
  event?: string;
  level?: "debug" | "info" | "warn" | "error";
  includeExpired?: boolean;
}): Promise<PipelineRunLogRow[]> {
  const pool = getPool();
  const result = await pool.query<{
    run_id: string;
    seq: number;
    level: "debug" | "info" | "warn" | "error";
    stage: string;
    event: string;
    message: string;
    payload_jsonb: Record<string, unknown>;
    created_at: Date | string;
    expires_at: Date | string;
  }>(
    `
      SELECT
        run_id,
        seq,
        level,
        stage,
        event,
        message,
        payload_jsonb,
        created_at,
        expires_at
      FROM pipeline_run_logs
      WHERE run_id = $1
        AND ($2::boolean = true OR expires_at > NOW())
        AND ($3::text IS NULL OR stage = $3)
        AND ($4::text IS NULL OR event = $4)
        AND ($5::text IS NULL OR level = $5)
      ORDER BY seq DESC
      LIMIT $6
    `,
    [
      input.runId,
      Boolean(input.includeExpired),
      input.stage ?? null,
      input.event ?? null,
      input.level ?? null,
      input.limit,
    ],
  );

  return result.rows.map(mapRunLogRow).reverse();
}

export async function exportRunLogs(input: {
  runId: string;
  includeExpired?: boolean;
}): Promise<PipelineRunLogRow[]> {
  const pool = getPool();
  const result = await pool.query<{
    run_id: string;
    seq: number;
    level: "debug" | "info" | "warn" | "error";
    stage: string;
    event: string;
    message: string;
    payload_jsonb: Record<string, unknown>;
    created_at: Date | string;
    expires_at: Date | string;
  }>(
    `
      SELECT
        run_id,
        seq,
        level,
        stage,
        event,
        message,
        payload_jsonb,
        created_at,
        expires_at
      FROM pipeline_run_logs
      WHERE run_id = $1
        AND ($2::boolean = true OR expires_at > NOW())
      ORDER BY seq ASC
    `,
    [input.runId, Boolean(input.includeExpired)],
  );

  return result.rows.map(mapRunLogRow);
}

export async function cleanupExpiredRunLogs(retentionHours: number): Promise<number> {
  const pool = getPool();
  const result = await pool.query(
    `
      DELETE FROM pipeline_run_logs
      WHERE expires_at <= NOW()
         OR created_at <= NOW() - (($1::text || ' hours')::interval)
    `,
    [retentionHours],
  );

  return result.rowCount ?? 0;
}

async function upsertCategory(
  client: PoolClient,
  storeId: string,
  draft: CategoryDraft,
): Promise<PersistedCategory> {
  const result = await client.query<PersistedCategory>(
    `
      INSERT INTO categories (store_id, name_pt, slug, description_pt, attributes_jsonb)
      VALUES ($1, $2, $3, $4, $5::jsonb)
      ON CONFLICT (store_id, slug)
      DO UPDATE SET
        name_pt = EXCLUDED.name_pt,
        description_pt = EXCLUDED.description_pt,
        attributes_jsonb = EXCLUDED.attributes_jsonb,
        updated_at = NOW()
      RETURNING id, slug, name_pt, description_pt, attributes_jsonb
    `,
    [storeId, draft.name_pt, draft.slug, draft.description_pt, JSON.stringify(draft.attributes_jsonb)],
  );

  const category = result.rows[0];

  await client.query(
    `
      DELETE FROM category_synonyms
      WHERE category_id = $1
        AND source = 'ai'
    `,
    [category.id],
  );

  for (const synonym of draft.synonyms) {
    await client.query(
      `
        INSERT INTO category_synonyms (category_id, term_pt, source)
        VALUES ($1, $2, 'ai')
        ON CONFLICT (category_id, term_pt) DO NOTHING
      `,
      [category.id, synonym],
    );
  }

  return {
    ...category,
    attributes_jsonb: draft.attributes_jsonb,
  };
}

export async function upsertCategoryDrafts(
  storeId: string,
  drafts: CategoryDraft[],
): Promise<Map<string, PersistedCategory>> {
  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const bySlug = new Map<string, PersistedCategory>();
    for (const draft of drafts) {
      const category = await upsertCategory(client, storeId, draft);
      bySlug.set(draft.slug, category);
    }

    await client.query("COMMIT");
    return bySlug;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function upsertProducts(input: {
  storeId: string;
  runId: string;
  products: NormalizedCatalogProduct[];
  enrichments: Map<string, ProductEnrichment>;
  categoriesBySlug: Map<string, PersistedCategory>;
  queryTimeoutMs?: number;
  batchSize?: number;
  stageTimeoutMs?: number;
  timeoutStageName?: string;
  onProgress?: (progress: { processed: number; total: number }) => void;
}): Promise<Map<string, PersistedProduct>> {
  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    if (input.queryTimeoutMs) {
      await client.query(`SET LOCAL statement_timeout = '${input.queryTimeoutMs}ms'`);
      await client.query(`SET LOCAL lock_timeout = '${Math.min(input.queryTimeoutMs, 5000)}ms'`);
    }

    const bySku = new Map<string, PersistedProduct>();
    const total = input.products.length;
    let processed = 0;
    const batchSize = Math.max(1, input.batchSize ?? 50);
    const stageStartedAt = Date.now();
    let nextProgressMilestone = total > 0 ? 1 : 0;

    const emitProgress = (currentProcessed: number) => {
      if (!input.onProgress || total === 0) {
        return;
      }

      while (nextProgressMilestone > 0 && nextProgressMilestone <= currentProcessed) {
        input.onProgress({
          processed: nextProgressMilestone,
          total,
        });

        if (nextProgressMilestone === 1) {
          nextProgressMilestone = 50;
          continue;
        }

        nextProgressMilestone += 50;
      }

      if (currentProcessed === total && (nextProgressMilestone === 0 || nextProgressMilestone > total)) {
        if (total > 1 && (total % 50 !== 0)) {
          input.onProgress({
            processed: total,
            total,
          });
        }
      }
    };

    for (let index = 0; index < input.products.length; index += batchSize) {
      if (input.stageTimeoutMs && Date.now() - stageStartedAt >= input.stageTimeoutMs) {
        throw new Error(
          `Stage '${input.timeoutStageName ?? "product_persist.products_upsert"}' exceeded timeout (${input.stageTimeoutMs}ms).`,
        );
      }

      const batch = input.products.slice(index, index + batchSize);
      const values: unknown[] = [];
      const placeholders: string[] = [];
      let position = 1;

      for (const product of batch) {
        const enrichment = input.enrichments.get(product.sourceSku);
        const category = enrichment
          ? input.categoriesBySlug.get(enrichment.categorySlug) ?? null
          : null;

        values.push(
          input.storeId,
          product.sourceSku,
          product.title,
          product.description ?? null,
          product.brand ?? null,
          product.price ?? null,
          product.availability ?? null,
          product.url ?? null,
          product.imageUrl ?? null,
          category?.id ?? null,
          enrichment?.categoryConfidence ?? 0,
          JSON.stringify(enrichment?.attributeValues ?? {}),
          JSON.stringify(enrichment?.attributeConfidence ?? {}),
          enrichment?.needsReview ?? true,
          input.runId,
        );

        placeholders.push(
          `($${position},$${position + 1},$${position + 2},$${position + 3},$${position + 4},$${position + 5},$${position + 6},$${position + 7},$${position + 8},$${position + 9},$${position + 10},$${position + 11}::jsonb,$${position + 12}::jsonb,$${position + 13},$${position + 14})`,
        );
        position += 15;
      }

      const result = await client.query<PersistedProduct>(
        `
          INSERT INTO products (
            store_id,
            source_sku,
            title,
            description,
            brand,
            price,
            availability,
            url,
            image_url,
            category_id,
            category_confidence,
            attribute_values_jsonb,
            attribute_confidence_jsonb,
            needs_review,
            run_id
          )
          VALUES ${placeholders.join(", ")}
          ON CONFLICT (store_id, source_sku)
          DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            brand = EXCLUDED.brand,
            price = EXCLUDED.price,
            availability = EXCLUDED.availability,
            url = EXCLUDED.url,
            image_url = EXCLUDED.image_url,
            category_id = EXCLUDED.category_id,
            category_confidence = EXCLUDED.category_confidence,
            attribute_values_jsonb = EXCLUDED.attribute_values_jsonb,
            attribute_confidence_jsonb = EXCLUDED.attribute_confidence_jsonb,
            needs_review = EXCLUDED.needs_review,
            run_id = EXCLUDED.run_id,
            updated_at = NOW()
          RETURNING id, source_sku, title, category_id, category_confidence, needs_review, attribute_values_jsonb
        `,
        values,
      );

      for (const row of result.rows) {
        bySku.set(row.source_sku, row);
      }

      processed += batch.length;
      if (input.stageTimeoutMs && Date.now() - stageStartedAt >= input.stageTimeoutMs && processed < total) {
        throw new Error(
          `Stage '${input.timeoutStageName ?? "product_persist.products_upsert"}' exceeded timeout (${input.stageTimeoutMs}ms).`,
        );
      }
      emitProgress(processed);
    }

    await client.query("COMMIT");
    return bySku;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export interface UpsertProductVectorsResult {
  totalRows: number;
  totalBatches: number;
}

export async function upsertProductVectors(input: {
  productBySku: Map<string, PersistedProduct>;
  vectorsBySku: Map<string, number[]>;
  embeddedTextBySku: Map<string, string>;
  embeddingModel: string;
  queryTimeoutMs: number;
  batchSize: number;
  stageTimeoutMs?: number;
  timeoutStageName?: string;
  onBatchProgress?: (progress: {
    processedBatches: number;
    totalBatches: number;
    processedRows: number;
    totalRows: number;
  }) => void;
}): Promise<UpsertProductVectorsResult> {
  const pool = getPool();
  const client = await pool.connect();
  const rows: Array<{
    productId: string;
    vectorLiteral: string;
    embeddedText: string;
  }> = [];

  for (const [sku, persistedProduct] of input.productBySku.entries()) {
    const vector = input.vectorsBySku.get(sku);
    const text = input.embeddedTextBySku.get(sku);
    if (!vector || !text) {
      continue;
    }

    rows.push({
      productId: persistedProduct.id,
      vectorLiteral: vectorToSqlLiteral(vector),
      embeddedText: text,
    });
  }

  if (rows.length === 0) {
    client.release();
    return {
      totalRows: 0,
      totalBatches: 0,
    };
  }

  const totalRows = rows.length;
  const totalBatches = Math.ceil(totalRows / Math.max(1, input.batchSize));
  let processedRows = 0;
  let processedBatches = 0;
  const stageStartedAt = Date.now();

  try {
    await client.query(`SET statement_timeout = '${input.queryTimeoutMs}ms'`);
    await client.query(`SET lock_timeout = '${Math.min(input.queryTimeoutMs, 5000)}ms'`);

    const batchSize = Math.max(1, input.batchSize);
    for (let index = 0; index < rows.length; index += batchSize) {
      if (input.stageTimeoutMs && Date.now() - stageStartedAt >= input.stageTimeoutMs) {
        throw new Error(
          `Stage '${input.timeoutStageName ?? "product_persist.vectors_upsert"}' exceeded timeout (${input.stageTimeoutMs}ms).`,
        );
      }

      const batch = rows.slice(index, index + batchSize);
      const values: unknown[] = [];
      const placeholders: string[] = [];
      let position = 1;

      for (const row of batch) {
        values.push(row.productId, row.vectorLiteral, input.embeddingModel, row.embeddedText);
        placeholders.push(`($${position}, $${position + 1}::vector, $${position + 2}, $${position + 3})`);
        position += 4;
      }

      await client.query(
        `
          INSERT INTO product_vectors (product_id, embedding, embedding_model, embedded_text)
          VALUES ${placeholders.join(", ")}
          ON CONFLICT (product_id)
          DO UPDATE SET
            embedding = EXCLUDED.embedding,
            embedding_model = EXCLUDED.embedding_model,
            embedded_text = EXCLUDED.embedded_text,
            updated_at = NOW()
        `,
        values,
      );

      processedRows += batch.length;
      processedBatches += 1;
      if (input.stageTimeoutMs && Date.now() - stageStartedAt >= input.stageTimeoutMs && processedRows < totalRows) {
        throw new Error(
          `Stage '${input.timeoutStageName ?? "product_persist.vectors_upsert"}' exceeded timeout (${input.stageTimeoutMs}ms).`,
        );
      }
      input.onBatchProgress?.({
        processedBatches,
        totalBatches,
        processedRows,
        totalRows,
      });
    }
    return {
      totalRows,
      totalBatches,
    };
  } finally {
    try {
      await client.query("SET statement_timeout = DEFAULT");
      await client.query("SET lock_timeout = DEFAULT");
    } catch {
      // ignore cleanup failures on release path
    }
    client.release();
  }
}

export async function updateRunStatusFromQAEvaluation(input: {
  runId: string;
  passRate: number;
  threshold: number;
  stats: Record<string, unknown>;
}): Promise<void> {
  const status = input.passRate >= input.threshold ? "accepted" : "rejected";
  await finalizePipelineRun({
    runId: input.runId,
    status,
    stats: {
      ...input.stats,
      qa_pass_rate: input.passRate,
      qa_threshold: input.threshold,
    },
  });
}

export interface QaFeedbackImportRow {
  runId: string;
  sourceSku: string;
  predictedCategory: string;
  correctedCategory: string | null;
  correctedAttributesJson: Record<string, unknown>;
  reviewStatus: "pass" | "fail" | "skip";
  reviewNotes: string | null;
}

export async function upsertQaFeedbackRows(rows: QaFeedbackImportRow[]): Promise<number> {
  if (rows.length === 0) {
    return 0;
  }

  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    let upserted = 0;
    for (const row of rows) {
      const result = await client.query(
        `
          INSERT INTO pipeline_qa_feedback (
            run_id,
            source_sku,
            predicted_category,
            corrected_category,
            corrected_attributes_json,
            review_status,
            review_notes
          )
          VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
          ON CONFLICT (run_id, source_sku)
          DO UPDATE SET
            predicted_category = EXCLUDED.predicted_category,
            corrected_category = EXCLUDED.corrected_category,
            corrected_attributes_json = EXCLUDED.corrected_attributes_json,
            review_status = EXCLUDED.review_status,
            review_notes = EXCLUDED.review_notes,
            imported_at = NOW()
        `,
        [
          row.runId,
          row.sourceSku,
          row.predictedCategory,
          row.correctedCategory,
          JSON.stringify(row.correctedAttributesJson),
          row.reviewStatus,
          row.reviewNotes,
        ],
      );

      upserted += result.rowCount ?? 0;
    }

    await client.query("COMMIT");
    return upserted;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

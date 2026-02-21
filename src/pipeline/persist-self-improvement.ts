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

export interface StaleSelfImprovementRecoveryResult {
  recoveredRuns: number;
  requeuedBatches: number;
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
export async function enqueueSelfImprovementBatch(input: {
  requestedCount: number;
  loopType: SelfImproveLoopType;
  maxLoopsCap: number;
  retryLimit: number;
  autoApplyPolicy: SelfImproveAutoApplyPolicy;
}): Promise<SelfImprovementBatchDetails> {
  if (!Number.isInteger(input.requestedCount) || input.requestedCount <= 0) {
    throw new Error("requestedCount must be a positive integer.");
  }
  if (input.requestedCount > input.maxLoopsCap) {
    throw new Error(
      `requestedCount (${input.requestedCount}) exceeds maxLoopsCap (${input.maxLoopsCap}).`,
    );
  }

  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const batchResult = await client.query<SelfImprovementBatchQueryRow>(
      `
        INSERT INTO self_improvement_batches (
          requested_count,
          loop_type,
          status,
          max_loops_cap,
          retry_limit,
          auto_apply_policy,
          summary_jsonb
        )
        VALUES ($1, $2, 'queued', $3, $4, $5, $6::jsonb)
        RETURNING
          id,
          requested_count,
          loop_type,
          status,
          max_loops_cap,
          retry_limit,
          auto_apply_policy,
          created_at,
          started_at,
          finished_at,
          summary_jsonb
      `,
      [
        input.requestedCount,
        input.loopType,
        input.maxLoopsCap,
        input.retryLimit,
        input.autoApplyPolicy,
        JSON.stringify({
          total_loops: input.requestedCount,
          completed_loops: 0,
          failed_loops: 0,
          success_count: 0,
          retried_success_count: 0,
          final_failed_count: 0,
          gate_pass_rate: 0,
          auto_applied_updates_count: 0,
        } satisfies SelfImprovementBatchSummary),
      ],
    );

    if (batchResult.rowCount === 0) {
      throw new Error("Failed to enqueue self-improvement batch.");
    }

    const batch = batchResult.rows[0];
    const runs: SelfImprovementRunItem[] = [];

    for (let sequenceNo = 1; sequenceNo <= input.requestedCount; sequenceNo += 1) {
      const runResult = await client.query<SelfImprovementRunQueryRow>(
        `
          INSERT INTO self_improvement_batch_runs (
            batch_id,
            sequence_no,
            attempt_no,
            status
          )
          VALUES ($1, $2, 1, 'queued')
          RETURNING
            id,
            batch_id,
            sequence_no,
            attempt_no,
            status,
            pipeline_run_id,
            error_jsonb,
            self_correction_context_jsonb,
            gate_result_jsonb,
            learning_result_jsonb,
            started_at,
            finished_at
        `,
        [batch.id, sequenceNo],
      );
      runs.push(mapSelfImprovementRunRow(runResult.rows[0]));
    }

    await client.query("COMMIT");
    return {
      ...mapSelfImprovementBatchRow(batch),
      runs,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function listSelfImprovementBatches(input?: {
  limit?: number;
  includeFinished?: boolean;
}): Promise<SelfImprovementBatchListItem[]> {
  const pool = getPool();
  const limit = Math.max(1, Math.floor(input?.limit ?? 20));

  const result = await pool.query<SelfImprovementBatchQueryRow>(
    `
      SELECT
        id,
        requested_count,
        loop_type,
        status,
        max_loops_cap,
        retry_limit,
        auto_apply_policy,
        created_at,
        started_at,
        finished_at,
        summary_jsonb
      FROM self_improvement_batches
      WHERE (
        $1::boolean = true
        OR status IN ('queued', 'running')
      )
      ORDER BY created_at DESC
      LIMIT $2
    `,
    [Boolean(input?.includeFinished), limit],
  );

  return result.rows.map(mapSelfImprovementBatchRow);
}

export async function getSelfImprovementBatchDetails(
  batchId: string,
): Promise<SelfImprovementBatchDetails | null> {
  const pool = getPool();

  const batchResult = await pool.query<SelfImprovementBatchQueryRow>(
    `
      SELECT
        id,
        requested_count,
        loop_type,
        status,
        max_loops_cap,
        retry_limit,
        auto_apply_policy,
        created_at,
        started_at,
        finished_at,
        summary_jsonb
      FROM self_improvement_batches
      WHERE id = $1
      LIMIT 1
    `,
    [batchId],
  );

  if (batchResult.rowCount === 0) {
    return null;
  }

  const runsResult = await pool.query<SelfImprovementRunQueryRow>(
    `
      SELECT
        id,
        batch_id,
        sequence_no,
        attempt_no,
        status,
        pipeline_run_id,
        error_jsonb,
        self_correction_context_jsonb,
        gate_result_jsonb,
        learning_result_jsonb,
        started_at,
        finished_at
      FROM self_improvement_batch_runs
      WHERE batch_id = $1
      ORDER BY sequence_no ASC, attempt_no ASC
    `,
    [batchId],
  );

  return {
    ...mapSelfImprovementBatchRow(batchResult.rows[0]),
    runs: runsResult.rows.map(mapSelfImprovementRunRow),
  };
}

export async function cancelSelfImprovementBatch(
  batchId: string,
): Promise<SelfImprovementBatchListItem | null> {
  const pool = getPool();
  const result = await pool.query<SelfImprovementBatchQueryRow>(
    `
      UPDATE self_improvement_batches
      SET
        status = 'cancelled',
        finished_at = COALESCE(finished_at, NOW())
      WHERE id = $1
        AND status IN ('queued', 'running')
      RETURNING
        id,
        requested_count,
        loop_type,
        status,
        max_loops_cap,
        retry_limit,
        auto_apply_policy,
        created_at,
        started_at,
        finished_at,
        summary_jsonb
    `,
    [batchId],
  );

  if (result.rowCount === 0) {
    return null;
  }

  return mapSelfImprovementBatchRow(result.rows[0]);
}

export async function claimNextQueuedSelfImprovementBatch(): Promise<SelfImprovementBatchListItem | null> {
  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const result = await client.query<SelfImprovementBatchQueryRow>(
      `
        WITH candidate AS (
          SELECT id
          FROM self_improvement_batches
          WHERE status = 'queued'
          ORDER BY created_at ASC
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        )
        UPDATE self_improvement_batches AS batch
        SET
          status = 'running',
          started_at = COALESCE(batch.started_at, NOW())
        FROM candidate
        WHERE batch.id = candidate.id
        RETURNING
          batch.id,
          batch.requested_count,
          batch.loop_type,
          batch.status,
          batch.max_loops_cap,
          batch.retry_limit,
          batch.auto_apply_policy,
          batch.created_at,
          batch.started_at,
          batch.finished_at,
          batch.summary_jsonb
      `,
    );

    await client.query("COMMIT");
    if (result.rowCount === 0) {
      return null;
    }

    return mapSelfImprovementBatchRow(result.rows[0]);
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function recoverStaleSelfImprovementBatches(input: {
  staleAfterMinutes: number;
}): Promise<StaleSelfImprovementRecoveryResult> {
  if (!Number.isInteger(input.staleAfterMinutes) || input.staleAfterMinutes <= 0) {
    throw new Error("staleAfterMinutes must be a positive integer.");
  }

  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const staleRunResult = await client.query<{ batch_id: string }>(
      `
        WITH stale_runs AS (
          SELECT
            run.id,
            run.batch_id,
            run.attempt_no,
            batch.retry_limit
          FROM self_improvement_batch_runs AS run
          INNER JOIN self_improvement_batches AS batch
            ON batch.id = run.batch_id
          WHERE batch.status = 'running'
            AND run.status = 'running'
            AND run.started_at IS NOT NULL
            AND run.started_at <= NOW() - (($1::text || ' minutes')::interval)
          FOR UPDATE
        )
        UPDATE self_improvement_batch_runs AS run
        SET
          status = (
            CASE
              WHEN stale_runs.attempt_no > stale_runs.retry_limit THEN 'retried_failed'::text
              ELSE 'failed'::text
            END
          ),
          error_jsonb = COALESCE(run.error_jsonb, '{}'::jsonb) || jsonb_build_object(
            'message', 'stale_run_recovered_after_worker_interrupt',
            'recovered_at', NOW(),
            'stale_timeout_minutes', $1::int
          ),
          self_correction_context_jsonb = COALESCE(run.self_correction_context_jsonb, '{}'::jsonb)
            || jsonb_build_object(
              'failureSummary', 'Loop attempt recovered after worker interruption.',
              'staleRecoveredAt', NOW(),
              'staleTimeoutMinutes', $1::int
            ),
          finished_at = COALESCE(run.finished_at, NOW())
        FROM stale_runs
        WHERE run.id = stale_runs.id
        RETURNING run.batch_id
      `,
      [input.staleAfterMinutes],
    );

    const recoveredBatchIds = [...new Set(staleRunResult.rows.map((row) => row.batch_id))];

    const requeueResult = await client.query(
      `
        UPDATE self_improvement_batches AS batch
        SET
          status = 'queued',
          finished_at = NULL,
          summary_jsonb = COALESCE(batch.summary_jsonb, '{}'::jsonb) || jsonb_build_object(
            'running_sequence', NULL,
            'stale_recovery', jsonb_build_object(
              'recovered_at', NOW(),
              'stale_timeout_minutes', $1::int
            )
          )
        WHERE batch.status = 'running'
          AND (
            (
              batch.id = ANY($2::uuid[])
            )
            OR (
              batch.started_at IS NOT NULL
              AND batch.started_at <= NOW() - (($1::text || ' minutes')::interval)
              AND NOT EXISTS (
                SELECT 1
                FROM self_improvement_batch_runs AS run
                WHERE run.batch_id = batch.id
                  AND run.status = 'running'
              )
            )
          )
      `,
      [input.staleAfterMinutes, recoveredBatchIds],
    );

    await client.query("COMMIT");
    return {
      recoveredRuns: staleRunResult.rowCount ?? 0,
      requeuedBatches: requeueResult.rowCount ?? 0,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function startSelfImprovementRunAttempt(input: {
  batchId: string;
  sequenceNo: number;
  attemptNo: number;
}): Promise<SelfImprovementRunItem> {
  const pool = getPool();
  const result = await pool.query<SelfImprovementRunQueryRow>(
    `
      INSERT INTO self_improvement_batch_runs (
        batch_id,
        sequence_no,
        attempt_no,
        status,
        started_at,
        finished_at
      )
      VALUES ($1, $2, $3, 'running', NOW(), NULL)
      ON CONFLICT (batch_id, sequence_no, attempt_no)
      DO UPDATE SET
        status = 'running',
        started_at = COALESCE(self_improvement_batch_runs.started_at, NOW()),
        finished_at = NULL
      RETURNING
        id,
        batch_id,
        sequence_no,
        attempt_no,
        status,
        pipeline_run_id,
        error_jsonb,
        self_correction_context_jsonb,
        gate_result_jsonb,
        learning_result_jsonb,
        started_at,
        finished_at
    `,
    [input.batchId, input.sequenceNo, input.attemptNo],
  );

  return mapSelfImprovementRunRow(result.rows[0]);
}

export async function finalizeSelfImprovementRunAttempt(input: {
  batchId: string;
  sequenceNo: number;
  attemptNo: number;
  status: SelfImproveRunStatus;
  pipelineRunId?: string | null;
  error?: Record<string, unknown>;
  selfCorrectionContext?: SelfCorrectionContext | Record<string, unknown>;
  gateResult?: Record<string, unknown>;
  learningResult?: Record<string, unknown>;
}): Promise<SelfImprovementRunItem> {
  const pool = getPool();
  const result = await pool.query<SelfImprovementRunQueryRow>(
    `
      UPDATE self_improvement_batch_runs
      SET
        status = $4,
        pipeline_run_id = $5,
        error_jsonb = $6::jsonb,
        self_correction_context_jsonb = $7::jsonb,
        gate_result_jsonb = $8::jsonb,
        learning_result_jsonb = $9::jsonb,
        finished_at = NOW()
      WHERE batch_id = $1
        AND sequence_no = $2
        AND attempt_no = $3
      RETURNING
        id,
        batch_id,
        sequence_no,
        attempt_no,
        status,
        pipeline_run_id,
        error_jsonb,
        self_correction_context_jsonb,
        gate_result_jsonb,
        learning_result_jsonb,
        started_at,
        finished_at
    `,
    [
      input.batchId,
      input.sequenceNo,
      input.attemptNo,
      input.status,
      input.pipelineRunId ?? null,
      JSON.stringify(input.error ?? {}),
      JSON.stringify(input.selfCorrectionContext ?? {}),
      JSON.stringify(input.gateResult ?? {}),
      JSON.stringify(input.learningResult ?? {}),
    ],
  );

  if (result.rowCount === 0) {
    throw new Error(
      `Could not finalize self-improvement run attempt (${input.batchId}/${input.sequenceNo}/${input.attemptNo}).`,
    );
  }

  return mapSelfImprovementRunRow(result.rows[0]);
}

export async function finalizeSelfImprovementBatch(input: {
  batchId: string;
  status: Extract<SelfImproveBatchStatus, "completed" | "completed_with_failures" | "failed" | "cancelled">;
  summary: SelfImprovementBatchSummary;
}): Promise<SelfImprovementBatchListItem> {
  const pool = getPool();
  const result = await pool.query<SelfImprovementBatchQueryRow>(
    `
      UPDATE self_improvement_batches
      SET
        status = $2,
        summary_jsonb = $3::jsonb,
        finished_at = COALESCE(finished_at, NOW())
      WHERE id = $1
      RETURNING
        id,
        requested_count,
        loop_type,
        status,
        max_loops_cap,
        retry_limit,
        auto_apply_policy,
        created_at,
        started_at,
        finished_at,
        summary_jsonb
    `,
    [input.batchId, input.status, JSON.stringify(input.summary ?? {})],
  );

  if (result.rowCount === 0) {
    throw new Error(`Could not finalize self-improvement batch ${input.batchId}.`);
  }

  return mapSelfImprovementBatchRow(result.rows[0]);
}

export async function updateSelfImprovementBatchSummary(input: {
  batchId: string;
  summary: SelfImprovementBatchSummary;
}): Promise<SelfImprovementBatchListItem> {
  const pool = getPool();
  const result = await pool.query<SelfImprovementBatchQueryRow>(
    `
      UPDATE self_improvement_batches
      SET summary_jsonb = $2::jsonb
      WHERE id = $1
      RETURNING
        id,
        requested_count,
        loop_type,
        status,
        max_loops_cap,
        retry_limit,
        auto_apply_policy,
        created_at,
        started_at,
        finished_at,
        summary_jsonb
    `,
    [input.batchId, JSON.stringify(input.summary ?? {})],
  );

  if (result.rowCount === 0) {
    throw new Error(`Could not update self-improvement batch summary for ${input.batchId}.`);
  }

  return mapSelfImprovementBatchRow(result.rows[0]);
}

export async function insertLearningProposals(input: {
  batchId?: string | null;
  runId?: string | null;
  proposals: Array<{
    proposalKind: SelfImproveProposalKind;
    confidenceScore: number;
    expectedImpactScore: number;
    payload: SelfImproveProposalPayload;
    source?: Record<string, unknown>;
  }>;
}): Promise<SelfImproveProposal[]> {
  if (input.proposals.length === 0) {
    return [];
  }

  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const inserted: SelfImproveProposal[] = [];

    for (const proposal of input.proposals) {
      const result = await client.query<SelfImproveProposalQueryRow>(
        `
          INSERT INTO self_improvement_proposals (
            batch_id,
            run_id,
            proposal_kind,
            status,
            confidence_score,
            expected_impact_score,
            payload_jsonb,
            source_jsonb
          )
          VALUES ($1, $2, $3, 'proposed', $4, $5, $6::jsonb, $7::jsonb)
          RETURNING
            id,
            batch_id,
            run_id,
            proposal_kind,
            status,
            confidence_score,
            expected_impact_score,
            payload_jsonb,
            source_jsonb,
            created_at,
            updated_at
        `,
        [
          input.batchId ?? null,
          input.runId ?? null,
          proposal.proposalKind,
          proposal.confidenceScore,
          proposal.expectedImpactScore,
          JSON.stringify(proposal.payload),
          JSON.stringify(proposal.source ?? {}),
        ],
      );

      inserted.push(mapSelfImproveProposalRow(result.rows[0]));
    }

    await client.query("COMMIT");
    return inserted;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function listPendingLearningProposals(input: {
  batchId?: string;
  runId?: string;
  limit?: number;
}): Promise<SelfImproveProposal[]> {
  const pool = getPool();
  const limit = Math.max(1, Math.floor(input.limit ?? 200));
  const result = await pool.query<SelfImproveProposalQueryRow>(
    `
      SELECT
        id,
        batch_id,
        run_id,
        proposal_kind,
        status,
        confidence_score,
        expected_impact_score,
        payload_jsonb,
        source_jsonb,
        created_at,
        updated_at
      FROM self_improvement_proposals
      WHERE status = 'proposed'
        AND ($1::uuid IS NULL OR batch_id = $1)
        AND ($2::uuid IS NULL OR run_id = $2)
      ORDER BY expected_impact_score DESC, confidence_score DESC, created_at ASC
      LIMIT $3
    `,
    [input.batchId ?? null, input.runId ?? null, limit],
  );

  return result.rows.map(mapSelfImproveProposalRow);
}

export async function markProposalApplied(input: {
  proposalId: string;
  status?: Extract<SelfImproveProposalStatus, "applied" | "rejected" | "rolled_back">;
}): Promise<SelfImproveProposal> {
  const pool = getPool();
  const status = input.status ?? "applied";
  const result = await pool.query<SelfImproveProposalQueryRow>(
    `
      UPDATE self_improvement_proposals
      SET status = $2,
          updated_at = NOW()
      WHERE id = $1
      RETURNING
        id,
        batch_id,
        run_id,
        proposal_kind,
        status,
        confidence_score,
        expected_impact_score,
        payload_jsonb,
        source_jsonb,
        created_at,
        updated_at
    `,
    [input.proposalId, status],
  );

  if (result.rowCount === 0) {
    throw new Error(`Proposal ${input.proposalId} not found.`);
  }

  return mapSelfImproveProposalRow(result.rows[0]);
}

export async function recordHarnessRun(input: {
  batchId?: string | null;
  runId?: string | null;
  benchmarkSnapshotId?: string | null;
  result: HarnessEvalResult;
}): Promise<string> {
  const pool = getPool();
  const result = await pool.query<{ id: string }>(
    `
      INSERT INTO self_improvement_harness_runs (
        batch_id,
        run_id,
        benchmark_snapshot_id,
        passed,
        metric_scores_jsonb,
        failed_metrics_jsonb,
        baseline_run_id,
        candidate_run_id
      )
      VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8)
      RETURNING id
    `,
    [
      input.batchId ?? null,
      input.runId ?? null,
      input.benchmarkSnapshotId ?? null,
      input.result.passed,
      JSON.stringify(input.result.metricScores ?? {}),
      JSON.stringify(input.result.failedMetrics ?? []),
      input.result.baselineRunId,
      input.result.candidateRunId,
    ],
  );

  return result.rows[0].id;
}

export async function createRollbackEvent(input: {
  appliedChangeId: string;
  reason: string;
  metadata?: Record<string, unknown>;
}): Promise<void> {
  const pool = getPool();
  await pool.query(
    `
      INSERT INTO self_improvement_rollback_events (
        applied_change_id,
        reason,
        metadata_jsonb
      )
      VALUES ($1, $2, $3::jsonb)
    `,
    [input.appliedChangeId, input.reason, JSON.stringify(input.metadata ?? {})],
  );
}

export async function createBenchmarkSnapshot(input: {
  storeId: string;
  source: Record<string, unknown>;
  rowCount: number;
  sampleSize: number;
  datasetHash: string;
}): Promise<BenchmarkSnapshot> {
  const pool = getPool();
  const result = await pool.query<BenchmarkSnapshotQueryRow>(
    `
      INSERT INTO self_improvement_benchmark_snapshots (
        store_id,
        source_jsonb,
        row_count,
        sample_size,
        dataset_hash
      )
      VALUES ($1, $2::jsonb, $3, $4, $5)
      RETURNING
        id,
        store_id,
        source_jsonb,
        row_count,
        sample_size,
        dataset_hash,
        created_at
    `,
    [
      input.storeId,
      JSON.stringify(input.source ?? {}),
      input.rowCount,
      input.sampleSize,
      input.datasetHash,
    ],
  );

  return mapBenchmarkSnapshotRow(result.rows[0]);
}

export async function getLatestBenchmarkSnapshot(storeId: string): Promise<BenchmarkSnapshot | null> {
  const pool = getPool();
  const result = await pool.query<BenchmarkSnapshotQueryRow>(
    `
      SELECT
        id,
        store_id,
        source_jsonb,
        row_count,
        sample_size,
        dataset_hash,
        created_at
      FROM self_improvement_benchmark_snapshots
      WHERE store_id = $1
      ORDER BY created_at DESC
      LIMIT 1
    `,
    [storeId],
  );

  if (result.rowCount === 0) {
    return null;
  }

  return mapBenchmarkSnapshotRow(result.rows[0]);
}

export async function getBenchmarkSnapshotById(snapshotId: string): Promise<BenchmarkSnapshot | null> {
  const pool = getPool();
  const result = await pool.query<BenchmarkSnapshotQueryRow>(
    `
      SELECT
        id,
        store_id,
        source_jsonb,
        row_count,
        sample_size,
        dataset_hash,
        created_at
      FROM self_improvement_benchmark_snapshots
      WHERE id = $1
      LIMIT 1
    `,
    [snapshotId],
  );

  if (result.rowCount === 0) {
    return null;
  }

  return mapBenchmarkSnapshotRow(result.rows[0]);
}

export async function applyTaxonomyAndRulePatchTransactional(input: {
  proposal: SelfImproveProposal;
  versionBefore: string;
  versionAfter: string;
  metadata?: Record<string, unknown>;
}): Promise<AppliedChangeRecord> {
  const originalContent = await readFile(CATEGORY_RULES_FILE_PATH, "utf8");
  const originalRules = await readCategoryRulesFile(CATEGORY_RULES_FILE_PATH);
  const { updatedRules, oldValue, newValue } = applyProposalPayloadToRules({
    rules: originalRules,
    payload: input.proposal.payload,
  });

  const rollbackToken = randomUUID();
  const pool = getPool();
  const client = await pool.connect();
  let fileUpdated = false;

  try {
    await client.query("BEGIN");

    await writeCategoryRulesFile(CATEGORY_RULES_FILE_PATH, updatedRules);
    fileUpdated = true;

    await client.query(
      `
        INSERT INTO self_improvement_proposal_diffs (
          proposal_id,
          diff_jsonb
        )
        VALUES ($1, $2::jsonb)
      `,
      [
        input.proposal.id,
        JSON.stringify({
          field: input.proposal.payload.field,
          action: input.proposal.payload.action,
          target_slug: input.proposal.payload.target_slug,
          old_value: oldValue,
          new_value: newValue,
          version_before: input.versionBefore,
          version_after: input.versionAfter,
        }),
      ],
    );

    const appliedResult = await client.query<AppliedChangeQueryRow>(
      `
        INSERT INTO self_improvement_applied_changes (
          proposal_id,
          proposal_kind,
          status,
          version_before,
          version_after,
          rollback_token,
          metadata_jsonb
        )
        VALUES ($1, $2, 'applied', $3, $4, $5, $6::jsonb)
        RETURNING
          id,
          proposal_id,
          proposal_kind,
          status,
          version_before,
          version_after,
          rollback_token,
          metadata_jsonb,
          applied_at
      `,
      [
        input.proposal.id,
        input.proposal.proposalKind,
        input.versionBefore,
        input.versionAfter,
        rollbackToken,
        JSON.stringify({
          ...(input.metadata ?? {}),
          field: input.proposal.payload.field,
          action: input.proposal.payload.action,
          target_slug: input.proposal.payload.target_slug,
          old_value: oldValue,
          new_value: newValue,
        }),
      ],
    );

    await client.query(
      `
        UPDATE self_improvement_proposals
        SET status = 'applied',
            updated_at = NOW()
        WHERE id = $1
      `,
      [input.proposal.id],
    );

    await client.query("COMMIT");
    return mapAppliedChangeRow(appliedResult.rows[0]);
  } catch (error) {
    await client.query("ROLLBACK");
    if (fileUpdated) {
      await writeCategoryRulesFile(
        CATEGORY_RULES_FILE_PATH,
        JSON.parse(originalContent) as CategoryRuleFile,
      );
    }
    throw error;
  } finally {
    client.release();
  }
}

export async function recordAppliedChangeWithoutPatch(input: {
  proposal: SelfImproveProposal;
  versionBefore: string;
  versionAfter: string;
  metadata?: Record<string, unknown>;
}): Promise<AppliedChangeRecord> {
  const rollbackToken = randomUUID();
  const pool = getPool();
  const result = await pool.query<AppliedChangeQueryRow>(
    `
      INSERT INTO self_improvement_applied_changes (
        proposal_id,
        proposal_kind,
        status,
        version_before,
        version_after,
        rollback_token,
        metadata_jsonb
      )
      VALUES ($1, $2, 'applied', $3, $4, $5, $6::jsonb)
      RETURNING
        id,
        proposal_id,
        proposal_kind,
        status,
        version_before,
        version_after,
        rollback_token,
        metadata_jsonb,
        applied_at
    `,
    [
      input.proposal.id,
      input.proposal.proposalKind,
      input.versionBefore,
      input.versionAfter,
      rollbackToken,
      JSON.stringify({
        ...(input.metadata ?? {}),
        synthetic_apply: true,
      }),
    ],
  );

  await markProposalApplied({
    proposalId: input.proposal.id,
    status: "applied",
  });

  return mapAppliedChangeRow(result.rows[0]);
}

export async function rollbackAppliedChangeTransactional(input: {
  appliedChangeId: string;
  reason: string;
  metadata?: Record<string, unknown>;
}): Promise<AppliedChangeRecord> {
  const pool = getPool();
  const client = await pool.connect();
  const originalContent = await readFile(CATEGORY_RULES_FILE_PATH, "utf8");
  let fileUpdated = false;

  try {
    await client.query("BEGIN");

    const appliedResult = await client.query<AppliedChangeQueryRow & { payload_jsonb: Record<string, unknown> }>(
      `
        SELECT
          applied.id,
          applied.proposal_id,
          applied.proposal_kind,
          applied.status,
          applied.version_before,
          applied.version_after,
          applied.rollback_token,
          applied.metadata_jsonb,
          applied.applied_at,
          proposal.payload_jsonb
        FROM self_improvement_applied_changes AS applied
        JOIN self_improvement_proposals AS proposal
          ON proposal.id = applied.proposal_id
        WHERE applied.id = $1
          AND applied.status = 'applied'
        LIMIT 1
      `,
      [input.appliedChangeId],
    );

    if (appliedResult.rowCount === 0) {
      throw new Error(`Applied change ${input.appliedChangeId} not found or already rolled back.`);
    }

    const applied = appliedResult.rows[0];
    const payload = applied.payload_jsonb as unknown as SelfImproveProposalPayload;
    const syntheticApply = Boolean(applied.metadata_jsonb?.synthetic_apply);
    const previousValue = (applied.metadata_jsonb?.old_value ?? undefined) as
      | string
      | number
      | string[]
      | undefined;

    if (!syntheticApply) {
      const parsedRules = await readCategoryRulesFile(CATEGORY_RULES_FILE_PATH);
      const { updatedRules } = applyProposalPayloadToRules({
        rules: parsedRules,
        payload: {
          ...payload,
          action:
            payload.field === "auto_min_confidence" || payload.field === "auto_min_margin"
              ? "set"
              : "set",
          value:
            previousValue !== undefined
              ? Array.isArray(previousValue)
                ? previousValue[0] ?? ""
                : previousValue
              : payload.value,
        },
        fallbackOldValue: previousValue,
      });

      if (Array.isArray(previousValue)) {
        const targetRule = updatedRules.categories.find((rule) => rule.slug === payload.target_slug);
        if (!targetRule) {
          throw new Error(`Cannot rollback change for unknown slug '${payload.target_slug}'.`);
        }
        if (
          payload.field === "include_any" ||
          payload.field === "exclude_any" ||
          payload.field === "strong_exclude_any"
        ) {
          targetRule[payload.field] = dedupeTerms(previousValue.map((value) => String(value)));
        }
      }

      await writeCategoryRulesFile(CATEGORY_RULES_FILE_PATH, updatedRules);
      fileUpdated = true;
    }

    await client.query(
      `
        UPDATE self_improvement_applied_changes
        SET status = 'rolled_back'
        WHERE id = $1
      `,
      [input.appliedChangeId],
    );

    await client.query(
      `
        UPDATE self_improvement_proposals
        SET status = 'rolled_back',
            updated_at = NOW()
        WHERE id = $1
      `,
      [applied.proposal_id],
    );

    await client.query(
      `
        INSERT INTO self_improvement_rollback_events (
          applied_change_id,
          reason,
          metadata_jsonb
        )
        VALUES ($1, $2, $3::jsonb)
      `,
      [input.appliedChangeId, input.reason, JSON.stringify(input.metadata ?? {})],
    );

    await client.query("COMMIT");

    const refreshed = await getAppliedChangeById(input.appliedChangeId);
    if (!refreshed) {
      throw new Error(`Could not reload applied change ${input.appliedChangeId} after rollback.`);
    }
    return refreshed;
  } catch (error) {
    await client.query("ROLLBACK");
    if (fileUpdated) {
      await writeCategoryRulesFile(
        CATEGORY_RULES_FILE_PATH,
        JSON.parse(originalContent) as CategoryRuleFile,
      );
    }
    throw error;
  } finally {
    client.release();
  }
}

export async function getAppliedChangeById(
  appliedChangeId: string,
): Promise<AppliedChangeRecord | null> {
  const pool = getPool();
  const result = await pool.query<AppliedChangeQueryRow>(
    `
      SELECT
        id,
        proposal_id,
        proposal_kind,
        status,
        version_before,
        version_after,
        rollback_token,
        metadata_jsonb,
        applied_at
      FROM self_improvement_applied_changes
      WHERE id = $1
      LIMIT 1
    `,
    [appliedChangeId],
  );

  if (result.rowCount === 0) {
    return null;
  }

  return mapAppliedChangeRow(result.rows[0]);
}

export async function listRecentAppliedChanges(input?: {
  status?: "applied" | "rolled_back";
  limit?: number;
}): Promise<AppliedChangeRecord[]> {
  const pool = getPool();
  const limit = Math.max(1, Math.floor(input?.limit ?? 50));
  const result = await pool.query<AppliedChangeQueryRow>(
    `
      SELECT
        id,
        proposal_id,
        proposal_kind,
        status,
        version_before,
        version_after,
        rollback_token,
        metadata_jsonb,
        applied_at
      FROM self_improvement_applied_changes
      WHERE ($1::text IS NULL OR status = $1)
      ORDER BY applied_at DESC
      LIMIT $2
    `,
    [input?.status ?? null, limit],
  );

  return result.rows.map(mapAppliedChangeRow);
}

export async function listRecentHarnessRuns(input?: {
  batchId?: string;
  limit?: number;
}): Promise<
  Array<{
    id: string;
    batchId: string | null;
    runId: string | null;
    benchmarkSnapshotId: string | null;
    passed: boolean;
    metricScores: Record<string, number>;
    failedMetrics: string[];
    baselineRunId: string | null;
    candidateRunId: string | null;
    createdAt: string;
  }>
> {
  const pool = getPool();
  const limit = Math.max(1, Math.floor(input?.limit ?? 50));
  const result = await pool.query<{
    id: string;
    batch_id: string | null;
    run_id: string | null;
    benchmark_snapshot_id: string | null;
    passed: boolean;
    metric_scores_jsonb: Record<string, number>;
    failed_metrics_jsonb: string[];
    baseline_run_id: string | null;
    candidate_run_id: string | null;
    created_at: Date | string;
  }>(
    `
      SELECT
        id,
        batch_id,
        run_id,
        benchmark_snapshot_id,
        passed,
        metric_scores_jsonb,
        failed_metrics_jsonb,
        baseline_run_id,
        candidate_run_id,
        created_at
      FROM self_improvement_harness_runs
      WHERE ($1::uuid IS NULL OR batch_id = $1)
      ORDER BY created_at DESC
      LIMIT $2
    `,
    [input?.batchId ?? null, limit],
  );

  return result.rows.map((row) => ({
    id: row.id,
    batchId: row.batch_id,
    runId: row.run_id,
    benchmarkSnapshotId: row.benchmark_snapshot_id,
    passed: row.passed,
    metricScores: row.metric_scores_jsonb ?? {},
    failedMetrics: Array.isArray(row.failed_metrics_jsonb) ? row.failed_metrics_jsonb : [],
    baselineRunId: row.baseline_run_id,
    candidateRunId: row.candidate_run_id,
    createdAt: toIsoString(row.created_at),
  }));
}

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

function mapSelfImprovementBatchRow(row: SelfImprovementBatchQueryRow): SelfImprovementBatchListItem {
  return {
    id: row.id,
    requestedCount: row.requested_count,
    loopType: row.loop_type,
    status: row.status,
    maxLoopsCap: row.max_loops_cap,
    retryLimit: row.retry_limit,
    autoApplyPolicy: row.auto_apply_policy,
    createdAt: toIsoString(row.created_at),
    startedAt: row.started_at ? toIsoString(row.started_at) : null,
    finishedAt: row.finished_at ? toIsoString(row.finished_at) : null,
    summary: (row.summary_jsonb ?? {}) as SelfImprovementBatchSummary,
  };
}

function mapSelfImprovementRunRow(row: SelfImprovementRunQueryRow): SelfImprovementRunItem {
  return {
    id: row.id,
    batchId: row.batch_id,
    sequenceNo: row.sequence_no,
    attemptNo: row.attempt_no,
    status: row.status,
    pipelineRunId: row.pipeline_run_id,
    error: row.error_jsonb ?? {},
    selfCorrectionContext: row.self_correction_context_jsonb ?? {},
    gateResult: row.gate_result_jsonb ?? {},
    learningResult: row.learning_result_jsonb ?? {},
    startedAt: row.started_at ? toIsoString(row.started_at) : null,
    finishedAt: row.finished_at ? toIsoString(row.finished_at) : null,
  };
}

function mapSelfImproveProposalRow(row: SelfImproveProposalQueryRow): SelfImproveProposal {
  return {
    id: row.id,
    batchId: row.batch_id,
    runId: row.run_id,
    proposalKind: row.proposal_kind,
    status: row.status,
    confidenceScore: row.confidence_score,
    expectedImpactScore: row.expected_impact_score,
    payload: row.payload_jsonb as unknown as SelfImproveProposalPayload,
    source: row.source_jsonb ?? {},
    createdAt: toIsoString(row.created_at),
    updatedAt: toIsoString(row.updated_at),
  };
}

function mapAppliedChangeRow(row: AppliedChangeQueryRow): AppliedChangeRecord {
  return {
    id: row.id,
    proposalId: row.proposal_id,
    proposalKind: row.proposal_kind,
    status: row.status,
    versionBefore: row.version_before,
    versionAfter: row.version_after,
    appliedAt: toIsoString(row.applied_at),
    rollbackToken: row.rollback_token,
    metadata: row.metadata_jsonb ?? {},
  };
}

function mapBenchmarkSnapshotRow(row: BenchmarkSnapshotQueryRow): BenchmarkSnapshot {
  return {
    id: row.id,
    storeId: row.store_id,
    source: row.source_jsonb ?? {},
    rowCount: row.row_count,
    sampleSize: row.sample_size,
    datasetHash: row.dataset_hash,
    createdAt: toIsoString(row.created_at),
  };
}

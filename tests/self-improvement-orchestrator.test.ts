import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  SelfImproveBatchStatus,
  SelfImproveLoopType,
  SelfImproveRunStatus,
} from "../src/types.js";

interface MockBatch {
  id: string;
  requestedCount: number;
  loopType: SelfImproveLoopType;
  status: SelfImproveBatchStatus;
  maxLoopsCap: number;
  retryLimit: number;
  autoApplyPolicy: "if_gate_passes";
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  summary: Record<string, unknown>;
  runs: Array<Record<string, unknown>>;
}

const mockState = vi.hoisted(() => ({
  claimQueue: [] as MockBatch[],
  batchesById: new Map<string, MockBatch>(),
  runSummaries: [] as Array<{ runId: string; artifacts?: Array<{ format: string; key: string; fileName: string }> }>,
  runStatsById: new Map<string, Record<string, unknown>>(),
  finalizeAttemptCalls: [] as Array<{
    batchId: string;
    sequenceNo: number;
    attemptNo: number;
    status: SelfImproveRunStatus;
    learningResult: Record<string, unknown>;
  }>,
  finalizeBatchCalls: [] as Array<{ batchId: string; status: SelfImproveBatchStatus; summary: Record<string, unknown> }>,
}));

function makeBatch(input: {
  id: string;
  requestedCount: number;
  loopType: SelfImproveLoopType;
  retryLimit?: number;
}): MockBatch {
  return {
    id: input.id,
    requestedCount: input.requestedCount,
    loopType: input.loopType,
    status: "running",
    maxLoopsCap: 10,
    retryLimit: input.retryLimit ?? 1,
    autoApplyPolicy: "if_gate_passes",
    createdAt: new Date().toISOString(),
    startedAt: new Date().toISOString(),
    finishedAt: null,
    summary: {
      total_loops: input.requestedCount,
      completed_loops: 0,
      failed_loops: 0,
      success_count: 0,
      retried_success_count: 0,
      final_failed_count: 0,
      gate_pass_rate: 0,
      auto_applied_updates_count: 0,
    },
    runs: [],
  };
}

function makeStats(input: {
  autoAcceptedRate: number;
  preQaPassed?: boolean;
  topConfusionAlerts?: Array<{
    category_slug: string;
    affected_count: number;
    low_margin_count: number;
    contradiction_count: number;
    fallback_count: number;
  }>;
}): Record<string, unknown> {
  return {
    auto_accepted_rate: input.autoAcceptedRate,
    fallback_category_rate: 0.01,
    needs_review_rate: 0.05,
    attribute_validation_fail_count: 0,
    unique_products_processed: 100,
    top_confusion_alerts: input.topConfusionAlerts ?? [],
    quality_gate: {
      auto_accepted_rate_target: 0.7,
      fallback_category_rate_target: 0.05,
      attribute_validation_fail_rate_target: 0.08,
      needs_review_rate_target: 0.3,
      pre_qa_passed: input.preQaPassed ?? true,
      status: input.preQaPassed === false ? "failed_pre_qa" : "pending_manual_qa",
    },
  };
}

vi.mock("../src/config.js", () => ({
  getConfig: () => ({
    CATALOG_INPUT_PATH: "./data/output.csv",
    STORE_ID: "continente",
    OUTPUT_DIR: "outputs",
    CANARY_SAMPLE_SIZE: 10,
    CANARY_FIXED_RATIO: 0.3,
    CANARY_RANDOM_SEED: "seed",
    CANARY_AUTO_ACCEPT_THRESHOLD: 0.8,
    CANARY_SUBSET_PATH: "outputs/canary_input.csv",
    CANARY_STATE_PATH: "outputs/canary_state.json",
    SELF_IMPROVE_MAX_STRUCTURAL_CHANGES_PER_LOOP: 2,
    SELF_IMPROVE_POST_APPLY_WATCH_LOOPS: 2,
    SELF_IMPROVE_ROLLBACK_ON_DEGRADE: true,
  }),
}));

vi.mock("../src/pipeline/run.js", () => ({
  runPipeline: vi.fn(async () => {
    const next = mockState.runSummaries.shift();
    if (!next) {
      throw new Error("No mocked run summary available.");
    }
    return {
      runId: next.runId,
      artifacts:
        next.artifacts ??
        [
          {
            format: "confusion-csv",
            key: "confusion_hotlist_csv",
            fileName: `confusion_hotlist_${next.runId}.csv`,
          },
        ],
    };
  }),
}));

vi.mock("../src/canary/select-subset.js", () => ({
  buildCanarySubset: vi.fn(async () => ({
    subsetPath: "/tmp/canary.csv",
    sampleSizeRequested: 10,
    sampleSizeUsed: 10,
    totalAvailable: 100,
    fixedTarget: 3,
    fixedSelected: 3,
    randomSelected: 7,
    hotlistSource: "none",
    hotlistPath: null,
    selectedSkus: [],
    warnings: [],
  })),
  writeCanaryState: vi.fn(async () => ({
    lastCanaryRunId: "mock-run",
    lastCanaryHotlistPath: "/tmp/hotlist.csv",
    updatedAt: new Date().toISOString(),
  })),
}));

vi.mock("../src/canary/gate.js", () => ({
  readAutoAcceptedRateFromStats: vi.fn((stats: Record<string, unknown>) => {
    return Number(stats.auto_accepted_rate ?? 0);
  }),
  isGatePassing: vi.fn((rate: number, threshold: number) => rate >= threshold),
}));

vi.mock("../src/pipeline/persist.js", () => ({
  claimNextQueuedSelfImprovementBatch: vi.fn(async () => mockState.claimQueue.shift() ?? null),
  getSelfImprovementBatchDetails: vi.fn(async (batchId: string) => mockState.batchesById.get(batchId) ?? null),
  startSelfImprovementRunAttempt: vi.fn(async () => ({
    id: "run-attempt",
  })),
  finalizeSelfImprovementRunAttempt: vi.fn(async (input: Record<string, unknown>) => {
    mockState.finalizeAttemptCalls.push({
      batchId: String(input.batchId),
      sequenceNo: Number(input.sequenceNo),
      attemptNo: Number(input.attemptNo),
      status: input.status as SelfImproveRunStatus,
      learningResult: (input.learningResult as Record<string, unknown>) ?? {},
    });
    return {
      id: "finalized",
    };
  }),
  updateSelfImprovementBatchSummary: vi.fn(async (input: Record<string, unknown>) => {
    const batch = mockState.batchesById.get(String(input.batchId));
    if (batch) {
      batch.summary = { ...(input.summary as Record<string, unknown>) };
    }
    return {
      id: input.batchId,
      summary: input.summary,
    };
  }),
  finalizeSelfImprovementBatch: vi.fn(async (input: Record<string, unknown>) => {
    const batch = mockState.batchesById.get(String(input.batchId));
    if (batch) {
      batch.status = input.status as SelfImproveBatchStatus;
      batch.summary = { ...(input.summary as Record<string, unknown>) };
    }
    mockState.finalizeBatchCalls.push({
      batchId: String(input.batchId),
      status: input.status as SelfImproveBatchStatus,
      summary: (input.summary as Record<string, unknown>) ?? {},
    });
    return {
      ...(batch ?? makeBatch({ id: String(input.batchId), requestedCount: 1, loopType: "full" })),
      status: input.status as SelfImproveBatchStatus,
      summary: input.summary as Record<string, unknown>,
    };
  }),
  getPipelineRunById: vi.fn(async (runId: string) => {
    const stats = mockState.runStatsById.get(runId);
    if (!stats) {
      return null;
    }
    return {
      runId,
      storeId: "continente",
      inputFileName: "input.csv",
      runLabel: null,
      status: "completed_pending_review",
      startedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
      stats,
    };
  }),
  recordHarnessRun: vi.fn(async () => "harness-run-1"),
}));

vi.mock("../src/pipeline/learning-proposal-generator.js", () => ({
  generateLearningProposals: vi.fn(async () => ({
    proposals: [
      {
        id: "proposal-1",
        payload: {
          target_slug: "escrita",
          field: "include_any",
          value: "caneta",
          reason: "qa_fail_correction_signal",
        },
      },
    ],
    summaryPatch: {
      proposals_generated: 1,
    },
  })),
}));

vi.mock("../src/pipeline/harness.js", () => ({
  evaluateHarnessForRun: vi.fn(async (input: { candidateRunId: string }) => ({
    benchmarkSnapshot: {
      id: "benchmark-1",
    },
    result: {
      passed: !input.candidateRunId.includes("harness-fail"),
      metricScores: {
        l1_delta: 0.01,
        l2_delta: 0.01,
        l3_delta: 0.01,
      },
      failedMetrics: input.candidateRunId.includes("harness-fail") ? ["l1_delta"] : [],
      baselineRunId: "baseline-run",
      candidateRunId: input.candidateRunId,
    },
  })),
}));

vi.mock("../src/pipeline/learning-apply.js", () => ({
  applyLearningProposals: vi.fn(async () => ({
    considered: 1,
    applied: 1,
    structuralApplied: 0,
    appliedChanges: [],
  })),
}));

vi.mock("../src/pipeline/learning-rollback.js", () => ({
  rollbackOnHarnessDegrade: vi.fn(async () => ({
    rolledBack: false,
    change: null,
  })),
}));

import {
  parseSelfImprovementPhrase,
  processNextSelfImprovementBatch,
} from "../src/pipeline/self-improvement-orchestrator.js";

describe("self-improvement phrase parsing", () => {
  it("parses canary and full run commands", () => {
    expect(parseSelfImprovementPhrase("run 5 self-improvement canary loops", 10)).toEqual({
      kind: "enqueue",
      count: 5,
      loopType: "canary",
    });

    expect(parseSelfImprovementPhrase("run 3 self-improvement full loop", 10)).toEqual({
      kind: "enqueue",
      count: 3,
      loopType: "full",
    });
  });

  it("parses status commands", () => {
    expect(parseSelfImprovementPhrase("show self-improvement batches", 10)).toEqual({
      kind: "status_all",
    });

    expect(parseSelfImprovementPhrase("show self-improvement batch abc-123", 10)).toEqual({
      kind: "status_one",
      batchId: "abc-123",
    });
  });

  it("rejects malformed or out-of-range commands", () => {
    expect(() => parseSelfImprovementPhrase("run 11 self-improvement full loops", 10)).toThrow(
      /exceeds max allowed/i,
    );
    expect(() => parseSelfImprovementPhrase("run bananas self-improvement loops", 10)).toThrow(
      /Unrecognized phrase/i,
    );
  });
});

describe("self-improvement batch processing", () => {
  beforeEach(() => {
    mockState.claimQueue = [];
    mockState.batchesById.clear();
    mockState.runSummaries = [];
    mockState.runStatsById.clear();
    mockState.finalizeAttemptCalls = [];
    mockState.finalizeBatchCalls = [];
  });

  it("processes queued batches in order", async () => {
    const batchA = makeBatch({ id: "batch-a", requestedCount: 1, loopType: "full" });
    const batchB = makeBatch({ id: "batch-b", requestedCount: 1, loopType: "full" });
    mockState.claimQueue.push(batchA, batchB);
    mockState.batchesById.set(batchA.id, batchA);
    mockState.batchesById.set(batchB.id, batchB);

    mockState.runSummaries.push({ runId: "run-a" }, { runId: "run-b" });
    mockState.runStatsById.set("run-a", makeStats({ autoAcceptedRate: 0.9 }));
    mockState.runStatsById.set("run-b", makeStats({ autoAcceptedRate: 0.9 }));

    const first = await processNextSelfImprovementBatch();
    const second = await processNextSelfImprovementBatch();

    expect(first.status).toBe("processed");
    expect(second.status).toBe("processed");
    expect(mockState.finalizeBatchCalls[0]?.batchId).toBe("batch-a");
    expect(mockState.finalizeBatchCalls[1]?.batchId).toBe("batch-b");
  });

  it("retries once and succeeds on retry", async () => {
    const batch = makeBatch({ id: "batch-retry-success", requestedCount: 1, loopType: "full" });
    mockState.claimQueue.push(batch);
    mockState.batchesById.set(batch.id, batch);

    mockState.runSummaries.push({ runId: "run-fail-gate" }, { runId: "run-pass-gate" });
    mockState.runStatsById.set("run-fail-gate", makeStats({ autoAcceptedRate: 0.5, preQaPassed: false }));
    mockState.runStatsById.set("run-pass-gate", makeStats({ autoAcceptedRate: 0.92 }));

    const outcome = await processNextSelfImprovementBatch();
    expect(outcome.status).toBe("processed");
    if (outcome.status === "processed") {
      expect(outcome.batchStatus).toBe("completed");
    }

    const statuses = mockState.finalizeAttemptCalls.map((call) => call.status);
    expect(statuses).toContain("failed");
    expect(statuses).toContain("retried_succeeded");
  });

  it("applies learning updates only when gate passes", async () => {
    const batch = makeBatch({ id: "batch-policy", requestedCount: 2, loopType: "full" });
    mockState.claimQueue.push(batch);
    mockState.batchesById.set(batch.id, batch);

    mockState.runSummaries.push({ runId: "run-pass" }, { runId: "run-fail" }, { runId: "run-fail-retry" });
    mockState.runStatsById.set(
      "run-pass",
      makeStats({
        autoAcceptedRate: 0.9,
        topConfusionAlerts: [
          {
            category_slug: "escrita",
            affected_count: 10,
            low_margin_count: 5,
            contradiction_count: 1,
            fallback_count: 0,
          },
        ],
      }),
    );
    mockState.runStatsById.set("run-fail", makeStats({ autoAcceptedRate: 0.4, preQaPassed: false }));
    mockState.runStatsById.set("run-fail-retry", makeStats({ autoAcceptedRate: 0.3, preQaPassed: false }));

    const outcome = await processNextSelfImprovementBatch();
    expect(outcome.status).toBe("processed");

    const successful = mockState.finalizeAttemptCalls.find((call) => call.status === "succeeded");
    expect(Number(successful?.learningResult.auto_applied_updates ?? 0)).toBeGreaterThan(0);

    const failed = mockState.finalizeAttemptCalls.find((call) => call.status === "failed");
    expect(Number(failed?.learningResult.auto_applied_updates ?? 0)).toBe(0);
  });

  it("handles mocked canary batch with mixed outcomes", async () => {
    const batch = makeBatch({ id: "batch-canary", requestedCount: 3, loopType: "canary" });
    mockState.claimQueue.push(batch);
    mockState.batchesById.set(batch.id, batch);

    mockState.runSummaries.push(
      { runId: "run-c1" },
      { runId: "run-c2-fail" },
      { runId: "run-c2-pass" },
      { runId: "run-c3-fail-1" },
      { runId: "run-c3-fail-2" },
    );

    mockState.runStatsById.set("run-c1", makeStats({ autoAcceptedRate: 0.9 }));
    mockState.runStatsById.set("run-c2-fail", makeStats({ autoAcceptedRate: 0.6, preQaPassed: false }));
    mockState.runStatsById.set("run-c2-pass", makeStats({ autoAcceptedRate: 0.85 }));
    mockState.runStatsById.set("run-c3-fail-1", makeStats({ autoAcceptedRate: 0.5, preQaPassed: false }));
    mockState.runStatsById.set("run-c3-fail-2", makeStats({ autoAcceptedRate: 0.55, preQaPassed: false }));

    const outcome = await processNextSelfImprovementBatch();
    expect(outcome.status).toBe("processed");
    if (outcome.status === "processed") {
      expect(outcome.batchStatus).toBe("completed_with_failures");
      expect(Number(outcome.summary.success_count ?? 0)).toBe(1);
      expect(Number(outcome.summary.retried_success_count ?? 0)).toBe(1);
      expect(Number(outcome.summary.final_failed_count ?? 0)).toBe(1);
    }
  });
});

import { beforeEach, describe, expect, it, vi } from "vitest";

const mockState = vi.hoisted(() => ({
  runs: new Map<string, { storeId: string; stats: Record<string, unknown>; status?: string }>(),
  recentByStore: new Map<string, Array<{ runId: string; status: string }>>(),
  latestSnapshot: {
    id: "snapshot-1",
    storeId: "store-a",
    source: { strategy: "qa_feedback_plus_hard_cases" },
    rowCount: 300,
    sampleSize: 300,
    datasetHash: "hash-1",
    createdAt: new Date().toISOString(),
  },
}));

vi.mock("../src/config.js", () => ({
  getConfig: () => ({
    SELF_IMPROVE_GATE_MIN_SAMPLE_SIZE: 50,
    HARNESS_MAX_FALLBACK_RATE: 0.06,
    HARNESS_MAX_NEEDS_REVIEW_RATE: 0.35,
    HARNESS_MIN_L1_DELTA: 0,
    HARNESS_MIN_L2_DELTA: 0,
    HARNESS_MIN_L3_DELTA: 0,
  }),
}));

vi.mock("../src/pipeline/persist.js", () => ({
  getPipelineRunById: vi.fn(async (runId: string) => {
    const run = mockState.runs.get(runId);
    if (!run) {
      return null;
    }
    return {
      runId,
      storeId: run.storeId,
      inputFileName: "input.csv",
      runLabel: null,
      status: run.status ?? "completed_pending_review",
      startedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
      stats: run.stats,
    };
  }),
  listPipelineRunsByStore: vi.fn(async (input: { storeId: string }) => {
    return (mockState.recentByStore.get(input.storeId) ?? []).map((run) => ({
      runId: run.runId,
      storeId: input.storeId,
      inputFileName: "input.csv",
      runLabel: null,
      status: run.status,
      startedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
      stats: {},
    }));
  }),
  getBenchmarkSnapshotById: vi.fn(async () => null),
  getLatestBenchmarkSnapshot: vi.fn(async () => mockState.latestSnapshot),
  createBenchmarkSnapshot: vi.fn(async () => mockState.latestSnapshot),
}));

import { evaluateHarnessForRun } from "../src/pipeline/harness.js";

describe("harness evaluation", () => {
  beforeEach(() => {
    mockState.runs.clear();
    mockState.recentByStore.clear();
    mockState.latestSnapshot = {
      id: "snapshot-1",
      storeId: "store-a",
      source: { strategy: "qa_feedback_plus_hard_cases" },
      rowCount: 300,
      sampleSize: 300,
      datasetHash: "hash-1",
      createdAt: new Date().toISOString(),
    };
  });

  it("fails when benchmark sample is below the configured minimum", async () => {
    mockState.latestSnapshot = {
      ...mockState.latestSnapshot,
      sampleSize: 20,
      rowCount: 20,
    };

    mockState.runs.set("candidate", {
      storeId: "store-a",
      stats: {
        fallback_category_rate: 0.02,
        needs_review_rate: 0.2,
        l1_accuracy: 0.9,
      },
    });
    mockState.runs.set("baseline", {
      storeId: "store-a",
      stats: {
        fallback_category_rate: 0.02,
        needs_review_rate: 0.2,
        l1_accuracy: 0.89,
      },
    });

    mockState.recentByStore.set("store-a", [
      { runId: "candidate", status: "accepted" },
      { runId: "baseline", status: "accepted" },
    ]);

    const outcome = await evaluateHarnessForRun({
      storeId: "store-a",
      candidateRunId: "candidate",
      baselineRunId: "baseline",
    });

    expect(outcome.result.passed).toBe(false);
    expect(outcome.result.failedMetrics).toContain("benchmark_sample_size");
  });

  it("passes when deltas and rates satisfy thresholds", async () => {
    mockState.runs.set("candidate", {
      storeId: "store-a",
      stats: {
        fallback_category_rate: 0.02,
        needs_review_rate: 0.2,
        l1_accuracy: 0.91,
        l2_accuracy: 0.84,
        l3_accuracy: 0.75,
      },
    });
    mockState.runs.set("baseline", {
      storeId: "store-a",
      stats: {
        fallback_category_rate: 0.03,
        needs_review_rate: 0.21,
        l1_accuracy: 0.9,
        l2_accuracy: 0.83,
        l3_accuracy: 0.74,
      },
    });

    mockState.recentByStore.set("store-a", [
      { runId: "candidate", status: "accepted" },
      { runId: "baseline", status: "accepted" },
    ]);

    const outcome = await evaluateHarnessForRun({
      storeId: "store-a",
      candidateRunId: "candidate",
      baselineRunId: "baseline",
    });

    expect(outcome.result.passed).toBe(true);
    expect(outcome.result.failedMetrics).toEqual([]);
    expect(outcome.result.metricScores.l1_delta).toBeGreaterThanOrEqual(0);
    expect(outcome.result.metricScores.candidate_fallback_category_rate).toBeLessThan(0.06);
  });
});
